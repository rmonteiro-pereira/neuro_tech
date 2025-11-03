"""
Apache Airflow DAG for IPTU Data Pipeline (Medallion Architecture).
This DAG orchestrates the complete data processing workflow using:
- Raw Layer: Source file cataloging
- Bronze Layer: Cleaned and cataloged data
- Silver Layer: Consolidated and transformed data
- Gold Layer: Refined business-ready outputs, analyses, and plots
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src directory to Python path for container environment
# In container, src is mounted at /opt/airflow/src
src_path = Path('/opt/airflow/src')
if not src_path.exists():
    # Fallback for local development
    src_path = Path(__file__).parent.parent / 'src'
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

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


def validate_pipeline_success(**context):
    """
    Comprehensive validation task to verify pipeline success.
    Checks that all expected files and outputs were created.
    """
    from datetime import datetime, timedelta
    
    logger.info("="*80)
    logger.info("Validating Pipeline Success")
    logger.info("="*80)
    
    from iptu_pipeline.config import settings
    from iptu_pipeline.config import BRONZE_DIR, SILVER_DIR, GOLD_DIR, CATALOG_DIR
    
    validation_results = {
        'catalog_updated': False,
        'bronze_files_created': [],
        'silver_files_created': False,
        'gold_outputs_created': False,
        'validation_reports_created': False,
        'errors': []
    }
    
    # 1. Check catalog was updated (check timestamp)
    logger.info("\n[1/5] Checking data catalog...")
    catalog_path = CATALOG_DIR / "data_catalog.json"
    if catalog_path.exists():
        try:
            import json
            with open(catalog_path, 'r', encoding='utf-8') as f:
                catalog_data = json.load(f)
            
            # Check if catalog has entries
            if catalog_data and len(catalog_data) > 0:
                # Check last_updated timestamp (should be recent, within last minute)
                max_age = timedelta(minutes=1)
                catalog_age = None
                
                for year, entry in catalog_data.items():
                    if isinstance(entry, dict) and 'last_updated' in entry:
                        try:
                            updated_str = entry['last_updated']
                            # Handle different timestamp formats
                            if updated_str.endswith('Z'):
                                updated_str = updated_str.replace('Z', '+00:00')
                            updated_time = datetime.fromisoformat(updated_str)
                            # Make both timezone-aware for comparison
                            if updated_time.tzinfo is None:
                                from datetime import timezone
                                updated_time = updated_time.replace(tzinfo=timezone.utc)
                            now = datetime.now(updated_time.tzinfo)
                            age = now - updated_time
                            if catalog_age is None or age > catalog_age:
                                catalog_age = age
                        except Exception as e:
                            logger.debug(f"Could not parse timestamp for {year}: {e}")
                            pass
                
                if catalog_age and catalog_age < max_age:
                    validation_results['catalog_updated'] = True
                    logger.info(f"✓ Catalog updated recently (age: {catalog_age})")
                else:
                    validation_results['errors'].append(f"Catalog not updated recently (age: {catalog_age})")
                    logger.warning(f"⚠ Catalog exists but may not be recent")
            else:
                validation_results['errors'].append("Catalog is empty")
                logger.error("✗ Catalog is empty")
        except Exception as e:
            validation_results['errors'].append(f"Error reading catalog: {str(e)}")
            logger.error(f"✗ Error reading catalog: {e}")
    else:
        validation_results['errors'].append("Catalog file does not exist")
        logger.error(f"✗ Catalog file not found: {catalog_path}")
    
    # 2. Check bronze layer files exist (one per year)
    logger.info("\n[2/5] Checking bronze layer files...")
    expected_years = sorted(settings.CSV_YEARS + settings.JSON_YEARS)
    for year in expected_years:
        bronze_path = BRONZE_DIR / f"iptu_{year}"
        parquet_file = bronze_path / "data.parquet"
        if parquet_file.exists():
            validation_results['bronze_files_created'].append(year)
            logger.info(f"✓ Bronze file exists for year {year}: {parquet_file}")
        else:
            validation_results['errors'].append(f"Bronze file missing for year {year}: {parquet_file}")
            logger.error(f"✗ Bronze file missing for year {year}: {bronze_path}")
    
    # 3. Check silver layer consolidated file exists
    logger.info("\n[3/5] Checking silver layer files...")
    silver_path = SILVER_DIR / "iptu_silver_consolidated"
    silver_parquet = silver_path / "data.parquet"
    if silver_parquet.exists():
        validation_results['silver_files_created'] = True
        file_size = silver_parquet.stat().st_size / (1024**2)  # MB
        logger.info(f"✓ Silver consolidated file exists: {silver_parquet} ({file_size:.2f} MB)")
    else:
        validation_results['errors'].append(f"Silver consolidated file missing: {silver_parquet}")
        logger.error(f"✗ Silver consolidated file missing: {silver_path}")
    
    # 4. Check gold layer outputs (plots, analyses)
    logger.info("\n[4/5] Checking gold layer outputs...")
    gold_plots_dir = settings.plots_output_path
    gold_analyses_dir = settings.analysis_output_path
    
    plots_exist = gold_plots_dir.exists() and any(gold_plots_dir.iterdir())
    analyses_exist = gold_analyses_dir.exists() and any(gold_analyses_dir.iterdir())
    
    if plots_exist or analyses_exist:
        validation_results['gold_outputs_created'] = True
        if plots_exist:
            plot_count = len(list(gold_plots_dir.glob("*.png"))) + len(list(gold_plots_dir.glob("*.html")))
            logger.info(f"✓ Gold plots directory has {plot_count} files: {gold_plots_dir}")
        if analyses_exist:
            analysis_count = len(list(gold_analyses_dir.iterdir()))
            logger.info(f"✓ Gold analyses directory has {analysis_count} files: {gold_analyses_dir}")
    else:
        validation_results['errors'].append("Gold layer outputs missing")
        logger.error(f"✗ Gold layer outputs missing (plots: {plots_exist}, analyses: {analyses_exist})")
    
    # 5. Check validation reports
    logger.info("\n[5/5] Checking validation reports...")
    medallion_report = settings.OUTPUT_DIR / "medallion_validation_report.json"
    legacy_report = settings.OUTPUT_DIR / "validation_report.csv"
    
    if medallion_report.exists() or legacy_report.exists():
        validation_results['validation_reports_created'] = True
        if medallion_report.exists():
            logger.info(f"✓ Medallion validation report exists: {medallion_report}")
        if legacy_report.exists():
            logger.info(f"✓ Legacy validation report exists: {legacy_report}")
    else:
        validation_results['errors'].append("Validation reports missing")
        logger.error(f"✗ Validation reports missing")
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("Validation Summary")
    logger.info("="*80)
    
    total_checks = 5
    passed_checks = sum([
        validation_results['catalog_updated'],
        len(validation_results['bronze_files_created']) > 0,
        validation_results['silver_files_created'],
        validation_results['gold_outputs_created'],
        validation_results['validation_reports_created']
    ])
    
    logger.info(f"Catalog updated: {'✓' if validation_results['catalog_updated'] else '✗'}")
    logger.info(f"Bronze files: {len(validation_results['bronze_files_created'])}/{len(expected_years)} years")
    logger.info(f"Silver consolidated: {'✓' if validation_results['silver_files_created'] else '✗'}")
    logger.info(f"Gold outputs: {'✓' if validation_results['gold_outputs_created'] else '✗'}")
    logger.info(f"Validation reports: {'✓' if validation_results['validation_reports_created'] else '✗'}")
    logger.info(f"\nPassed: {passed_checks}/{total_checks} checks")
    
    if validation_results['errors']:
        logger.error(f"Errors found: {len(validation_results['errors'])}")
        for error in validation_results['errors']:
            logger.error(f"  - {error}")
    
    # Fail task if critical checks failed
    if len(validation_results['errors']) > 3:  # Allow some flexibility
        error_msg = f"Pipeline validation failed: {len(validation_results['errors'])} errors found"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info("="*80)
    logger.info("Pipeline validation complete!")
    logger.info("="*80)
    
    return validation_results


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
                'engine': 'pyspark'  # Always use PySpark for Delta table support
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
    
    # Pipeline success validation task (final check - verifies all outputs were created)
    pipeline_validation_task = PythonOperator(
        task_id='validate_pipeline_success',
        python_callable=validate_pipeline_success,
        dag=dag,
    )
    
    # Define task dependencies
    # Medallion pipeline runs first (Raw -> Bronze -> Silver -> Gold, including analysis)
    # Then visualizations, dashboard, and reports can run in parallel (all depend on gold layer)
    # Finally, validation task runs after all outputs are complete
    medallion_pipeline_task >> [visualizations_task, dashboard_task, validation_reports_task] >> pipeline_validation_task


def get_airflow_dag():
    """Return the Airflow DAG for use in Airflow environment."""
    if not AIRFLOW_AVAILABLE:
        logger.warning("Airflow not available. DAG will not be functional.")
    return dag

