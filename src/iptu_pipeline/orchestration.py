"""
Orchestration module for IPTU pipeline.
Provides direct execution mode. For Airflow orchestration, use airflow_dag.py
"""
from pathlib import Path
from typing import Optional, List

from iptu_pipeline.utils.logger import setup_logger
from iptu_pipeline.pipelines.main_pipeline import IPTUPipeline
from iptu_pipeline.pipelines.medallion_pipeline import MedallionPipeline

logger = setup_logger("orchestration")


def run_orchestrated_pipeline(
    years: Optional[List[int]] = None,
    incremental: bool = False,
    run_analysis: bool = True,
    engine: Optional[str] = None
):
    """
    Run the orchestrated pipeline (direct execution mode).
    For Airflow orchestration, use the DAG defined in airflow_dag.py
    
    Args:
        years: Years to process. If None, processes all available years.
        incremental: Whether to run in incremental mode
        run_analysis: Whether to run analyses
        engine: Optional engine type ('pandas' or 'pyspark'). Uses config default if None.
    
    Returns:
        Consolidated DataFrame (Pandas or Spark)
    """
    logger.info("Running pipeline (direct execution mode)")
    logger.info("For Airflow orchestration, deploy the DAG from airflow_dag.py")
    
    pipeline = IPTUPipeline(engine=engine)
    return pipeline.run_full_pipeline(
        years=years,
        incremental=incremental,
        run_analysis=run_analysis
    )


def run_medallion_pipeline(
    years: Optional[List[int]] = None,
    incremental: bool = False,
    engine: Optional[str] = None
):
    """
    Run the medallion architecture pipeline (Raw -> Bronze -> Silver -> Gold).
    
    Args:
        years: Years to process. If None, processes all available years.
        incremental: Whether to run in incremental mode (only process new years)
        engine: Optional engine type ('pandas' or 'pyspark'). Uses config default if None.
    
    Returns:
        Dictionary with:
        - 'silver': Consolidated silver DataFrame (all years merged)
        - 'gold': Dictionary of gold layer outputs (multiple refined datasets)
    """
    logger.info("Running medallion architecture pipeline")
    
    from iptu_pipeline.engine import get_engine
    engine_instance = get_engine(engine or 'pyspark')  # Medallion works best with PySpark
    
    pipeline = MedallionPipeline(engine=engine_instance)
    return pipeline.run_full_medallion_pipeline(
        years=years,
        incremental=incremental
    )
