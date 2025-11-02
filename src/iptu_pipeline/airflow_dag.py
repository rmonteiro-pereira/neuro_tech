"""
Apache Airflow DAG for IPTU Data Pipeline.
This DAG orchestrates the complete data processing workflow.
"""
from datetime import datetime, timedelta
from pathlib import Path

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
    'iptu_data_pipeline',
    default_args=default_args,
    description='IPTU Data Processing Pipeline - Ingest, Transform, Analyze',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
    tags=['iptu', 'data-pipeline', 'etl'],
)


def validate_data_quality(**context):
    """Task to validate data quality for a specific year."""
    year = context.get('params', {}).get('year')
    if not year:
        raise ValueError("Year parameter required")
    
    logger.info(f"Validating data quality for year {year}")
    from iptu_pipeline.pipelines.ingestion import DataIngestion
    from iptu_pipeline.utils.data_quality import DataQualityValidator
    
    validator = DataQualityValidator()
    ingestion = DataIngestion(quality_validator=validator)
    
    df = ingestion.load_year_data(year, validate=True)
    validation_result = validator.validation_results[-1] if validator.validation_results else None
    
    if validation_result and not validation_result["passed"]:
        logger.warning(f"Validation failed for year {year}: {validation_result['errors']}")
    
    return validation_result


def ingest_data(**context):
    """Task to ingest data for specified years."""
    years = context.get('params', {}).get('years')
    incremental = context.get('params', {}).get('incremental', False)
    
    logger.info(f"Ingesting data for years: {years}, incremental: {incremental}")
    
    from iptu_pipeline.pipelines.ingestion import DataIngestion
    from iptu_pipeline.config import CONSOLIDATED_DATA_PATH
    
    ingestion = DataIngestion()
    
    if incremental:
        dataframes = ingestion.load_incremental(years, CONSOLIDATED_DATA_PATH)
    else:
        dataframes = ingestion.load_all_years(years=years)
    
    logger.info(f"Ingested {len(dataframes)} year(s)")
    return len(dataframes)


def transform_and_consolidate(**context):
    """Task to transform and consolidate data."""
    logger.info("Transforming and consolidating data")
    
    from iptu_pipeline.pipelines.transformation import DataTransformer
    from iptu_pipeline.pipelines.ingestion import DataIngestion
    from iptu_pipeline.config import CONSOLIDATED_DATA_PATH
    import pandas as pd
    
    # Get dataframes from ingestion
    ingestion = DataIngestion()
    dataframes = ingestion.get_ingestion_status()
    
    # For simplicity, reload all data
    # In production, you might want to pass dataframes through XCom
    years = sorted(dataframes.get('ingested_years', []))
    if not years:
        years = [2020, 2021, 2022, 2023, 2024]
    
    dataframes_dict = ingestion.load_all_years(years=years, validate=False)
    
    transformer = DataTransformer()
    
    # Check if we should append to existing
    append_to_existing = None
    if CONSOLIDATED_DATA_PATH.exists():
        try:
            existing_df = pd.read_parquet(CONSOLIDATED_DATA_PATH)
            existing_years = set(existing_df["ano do exercício"].unique())
            new_years = set(years) - existing_years
            if new_years and len(new_years) < len(years):
                # Incremental mode
                dataframes_dict = {y: dataframes_dict[y] for y in new_years}
                append_to_existing = CONSOLIDATED_DATA_PATH
        except Exception as e:
            logger.warning(f"Could not read existing data: {e}")
    
    consolidated_df = transformer.consolidate_datasets(dataframes_dict, append_to_existing)
    
    logger.info(f"Consolidated {len(consolidated_df):,} rows")
    return len(consolidated_df)


def save_consolidated_data(**context):
    """Task to save consolidated data."""
    logger.info("Saving consolidated data")
    
    from iptu_pipeline.config import CONSOLIDATED_DATA_PATH, PROCESSED_DATA_PATH
    from iptu_pipeline.pipelines.transformation import DataTransformer
    from iptu_pipeline.pipelines.ingestion import DataIngestion
    import pandas as pd
    
    # Reload and consolidate (in production, use XCom to pass data)
    ingestion = DataIngestion()
    years = [2020, 2021, 2022, 2023, 2024]
    dataframes = ingestion.load_all_years(years=years, validate=False)
    
    transformer = DataTransformer()
    consolidated_df = transformer.consolidate_datasets(dataframes)
    
    # Save to parquet
    consolidated_df.to_parquet(CONSOLIDATED_DATA_PATH, index=False, compression='snappy')
    consolidated_df.to_parquet(PROCESSED_DATA_PATH, index=False, compression='snappy')
    
    logger.info(f"Saved consolidated data: {len(consolidated_df):,} rows")
    return str(CONSOLIDATED_DATA_PATH)


def run_analysis(**context):
    """Task to run data analysis."""
    logger.info("Running data analysis")
    
    from iptu_pipeline.pipelines.analysis import IPTUAnalyzer
    from iptu_pipeline.config import CONSOLIDATED_DATA_PATH
    import pandas as pd
    
    # Load consolidated data
    df = pd.read_parquet(CONSOLIDATED_DATA_PATH)
    
    analyzer = IPTUAnalyzer(df)
    results = analyzer.generate_all_analyses()
    analyzer.save_analyses()
    
    logger.info("Analysis complete")
    return len(results)


def generate_dashboard(**context):
    """Task to generate dashboard."""
    logger.info("Generating dashboard")
    
    from iptu_pipeline.dashboard import IPTUDashboard
    from iptu_pipeline.config import CONSOLIDATED_DATA_PATH
    import pandas as pd
    
    # Load consolidated data
    df = pd.read_parquet(CONSOLIDATED_DATA_PATH)
    
    dashboard = IPTUDashboard(df=df)
    dashboard_path = dashboard.generate_dashboard_html()
    report_path = dashboard.generate_summary_report()
    
    logger.info(f"Dashboard generated: {dashboard_path}")
    return str(dashboard_path)


def generate_reports(**context):
    """Task to generate validation reports."""
    logger.info("Generating validation reports")
    
    from iptu_pipeline.utils.data_quality import DataQualityValidator
    from iptu_pipeline.config import OUTPUT_DIR
    
    # In production, validation results would come from XCom
    # For now, we'll generate a summary if reports exist
    report_path = OUTPUT_DIR / "validation_report.csv"
    
    if report_path.exists():
        logger.info(f"Validation report exists: {report_path}")
        return str(report_path)
    else:
        logger.warning("Validation report not found")
        return None


# Task definitions
if AIRFLOW_AVAILABLE:
    # Validation tasks for each year (can run in parallel)
    validate_2020 = PythonOperator(
        task_id='validate_data_quality_2020',
        python_callable=validate_data_quality,
        op_kwargs={'params': {'year': 2020}},
        dag=dag,
    )
    
    validate_2021 = PythonOperator(
        task_id='validate_data_quality_2021',
        python_callable=validate_data_quality,
        op_kwargs={'params': {'year': 2021}},
        dag=dag,
    )
    
    validate_2022 = PythonOperator(
        task_id='validate_data_quality_2022',
        python_callable=validate_data_quality,
        op_kwargs={'params': {'year': 2022}},
        dag=dag,
    )
    
    validate_2023 = PythonOperator(
        task_id='validate_data_quality_2023',
        python_callable=validate_data_quality,
        op_kwargs={'params': {'year': 2023}},
        dag=dag,
    )
    
    validate_2024 = PythonOperator(
        task_id='validate_data_quality_2024',
        python_callable=validate_data_quality,
        op_kwargs={'params': {'year': 2024}},
        dag=dag,
    )
    
    # Ingestion task (depends on all validations)
    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data,
        op_kwargs={'params': {'years': [2020, 2021, 2022, 2023, 2024], 'incremental': False}},
        dag=dag,
    )
    
    # Transform and consolidate task
    transform_task = PythonOperator(
        task_id='transform_and_consolidate',
        python_callable=transform_and_consolidate,
        dag=dag,
    )
    
    # Save consolidated data task
    save_task = PythonOperator(
        task_id='save_consolidated_data',
        python_callable=save_consolidated_data,
        dag=dag,
    )
    
    # Analysis task
    analysis_task = PythonOperator(
        task_id='run_analysis',
        python_callable=run_analysis,
        dag=dag,
    )
    
    # Dashboard generation task
    dashboard_task = PythonOperator(
        task_id='generate_dashboard',
        python_callable=generate_dashboard,
        dag=dag,
    )
    
    # Reports generation task
    reports_task = PythonOperator(
        task_id='generate_reports',
        python_callable=generate_reports,
        dag=dag,
    )
    
    # Define task dependencies
    [validate_2020, validate_2021, validate_2022, validate_2023, validate_2024] >> ingest_task
    ingest_task >> transform_task
    transform_task >> save_task
    save_task >> [analysis_task, reports_task]
    analysis_task >> dashboard_task


def get_airflow_dag():
    """Return the Airflow DAG for use in Airflow environment."""
    if not AIRFLOW_AVAILABLE:
        logger.warning("Airflow not available. DAG will not be functional.")
    return dag

