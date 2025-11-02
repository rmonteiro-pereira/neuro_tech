"""
Apache Airflow DAG for IPTU Data Pipeline (Medallion Architecture).
This DAG orchestrates the complete data processing workflow using:
- Raw Layer: Source file cataloging
- Bronze Layer: Cleaned and cataloged data
- Silver Layer: Consolidated and transformed data
- Gold Layer: Refined business-ready outputs, analyses, and plots
"""
from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    # Create mock classes if Airflow is not available
    class DAG:
        def __init__(self, *args, **kwargs):
            pass
    class PythonOperator:
        def __init__(self, *args, **kwargs):
            pass
    def days_ago(n):
        return datetime.now() - timedelta(days=n)

from iptu_pipeline.pipelines.main_pipeline import IPTUPipeline
from iptu_pipeline.utils.logger import setup_logger
from iptu_pipeline.config import settings

logger = setup_logger("airflow_dag")


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# DAG definition
dag = DAG(
    'iptu_medallion_pipeline',
    default_args=default_args,
    description='IPTU Data Processing Pipeline - Medallion Architecture (Raw -> Bronze -> Silver -> Gold)',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
    tags=['iptu', 'data-pipeline', 'medallion', 'bronze', 'silver', 'gold'],
)


def run_medallion_pipeline(**context):
    """
    Main task to run the complete medallion pipeline.
    Orchestrates: Raw (catalog) -> Bronze -> Silver -> Gold layers.
    """
    years = context.get('params', {}).get('years', None)
    incremental = context.get('params', {}).get('incremental', False)
    engine = context.get('params', {}).get('engine', None)
    
    logger.info(f"Running medallion pipeline: years={years}, incremental={incremental}, engine={engine}")
    
    pipeline = IPTUPipeline(engine=engine)
    consolidated_df = pipeline.run_full_pipeline(
        years=years,
        save_consolidated=True,  # Save legacy paths for backward compatibility
        run_analysis=True,
        incremental=incremental
    )
    
    # Get pipeline summary
    summary = pipeline.get_pipeline_summary()
    logger.info(f"Pipeline summary: {summary}")
    
    return {
        'row_count': pipeline.engine.get_count(consolidated_df),
        'column_count': len(pipeline.engine.get_columns(consolidated_df)),
        'summary': summary
    }


def generate_visualizations(**context):
    """Task to generate visualizations from analysis results (Gold layer)."""
    logger.info("Generating visualizations from gold layer analyses")
    
    from iptu_pipeline.visualizations import generate_plots_from_analysis_results
    from iptu_pipeline.config import settings
    
    plot_files = generate_plots_from_analysis_results(analysis_path=settings.analysis_output_path)
    
    logger.info(f"Generated {len([f for f in plot_files.keys() if f != 'html_report'])} plots")
    logger.info(f"HTML report: {plot_files.get('html_report', 'N/A')}")
    
    return plot_files.get('html_report', None)


def generate_dashboard(**context):
    """Task to generate interactive dashboard from gold layer data."""
    logger.info("Generating dashboard from gold layer")
    
    from iptu_pipeline.dashboard import IPTUDashboard
    from iptu_pipeline.config import SILVER_DIR
    from iptu_pipeline.engine import get_engine
    import pandas as pd
    
    engine = get_engine()
    
    # Load from silver layer (consolidated data)
    try:
        if engine.engine_type == "pyspark":
            df = engine.read_parquet(SILVER_DIR / "iptu_silver_consolidated")
            # Convert to Pandas for dashboard (dashboard currently only supports Pandas)
            df = df.toPandas()
        else:
            # Try to load from silver layer
            silver_path = SILVER_DIR / "iptu_silver_consolidated"
            if (silver_path / "data.parquet").exists():
                df = pd.read_parquet(silver_path / "data.parquet")
            else:
                # Fallback to legacy path
                from iptu_pipeline.config import CONSOLIDATED_DATA_PATH
                df = pd.read_parquet(CONSOLIDATED_DATA_PATH)
    except Exception as e:
        logger.warning(f"Could not load from silver layer: {e}, using legacy path")
        from iptu_pipeline.config import CONSOLIDATED_DATA_PATH
        df = pd.read_parquet(CONSOLIDATED_DATA_PATH)
    
    dashboard = IPTUDashboard(df=df)
    dashboard_path = dashboard.generate_dashboard()
    report_path = dashboard.generate_summary_report()
    
    logger.info(f"Dashboard generated: {dashboard_path}")
    logger.info(f"Summary report: {report_path}")
    return str(dashboard_path) if dashboard_path else None


def generate_validation_reports(**context):
    """Task to generate validation reports from medallion quality checks."""
    logger.info("Generating medallion validation reports")
    
    from iptu_pipeline.config import settings
    
    # Check for medallion validation report
    medallion_report_path = settings.OUTPUT_DIR / "medallion_validation_report.json"
    legacy_report_path = settings.OUTPUT_DIR / "validation_report.csv"
    
    reports = []
    
    if medallion_report_path.exists():
        reports.append(str(medallion_report_path))
        logger.info(f"Medallion validation report: {medallion_report_path}")
    
    if legacy_report_path.exists():
        reports.append(str(legacy_report_path))
        logger.info(f"Legacy validation report: {legacy_report_path}")
    
    return reports if reports else None


# Task definitions
if AIRFLOW_AVAILABLE:
    # Main medallion pipeline task (runs complete pipeline: Raw -> Bronze -> Silver -> Gold)
    medallion_pipeline_task = PythonOperator(
        task_id='run_medallion_pipeline',
        python_callable=run_medallion_pipeline,
        op_kwargs={
            'params': {
                'years': None,  # Process all years, or specify [2020, 2021, 2022, 2023, 2024]
                'incremental': False,  # Set to True for incremental processing
                'engine': None  # Uses config default, or specify 'pandas' or 'pyspark'
            }
        },
        dag=dag,
    )
    
    # Visualization generation task (depends on analysis from gold layer)
    visualizations_task = PythonOperator(
        task_id='generate_visualizations',
        python_callable=generate_visualizations,
        dag=dag,
    )
    
    # Dashboard generation task (depends on gold layer data)
    dashboard_task = PythonOperator(
        task_id='generate_dashboard',
        python_callable=generate_dashboard,
        dag=dag,
    )
    
    # Validation reports task (depends on medallion pipeline validation)
    validation_reports_task = PythonOperator(
        task_id='generate_validation_reports',
        python_callable=generate_validation_reports,
        dag=dag,
    )
    
    # Define task dependencies
    # Medallion pipeline runs first (Raw -> Bronze -> Silver -> Gold, including analysis)
    # Then visualizations, dashboard, and reports can run in parallel (all depend on gold layer)
    medallion_pipeline_task >> [visualizations_task, dashboard_task, validation_reports_task]


def get_airflow_dag():
    """Return the Airflow DAG for use in Airflow environment."""
    if not AIRFLOW_AVAILABLE:
        logger.warning("Airflow not available. DAG will not be functional.")
    return dag

