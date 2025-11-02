"""
Analysis module for IPTU pipeline.
Provides various analyses on the consolidated dataset.
Supports both Pandas and PySpark DataFrames.
"""
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import pandas as pd

try:
    from pyspark.sql import DataFrame as SparkDataFrame
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkDataFrame = Any

from iptu_pipeline.config import ANALYSIS_OUTPUT_PATH
from iptu_pipeline.engine import DataEngine, get_engine
from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("analysis")


class IPTUAnalyzer:
    """Performs analyses on IPTU data. Supports both Pandas and PySpark."""
    
    def __init__(self, df: Union[pd.DataFrame, SparkDataFrame], engine: Optional[str] = None):
        """
        Initialize analyzer with consolidated dataset.
        
        Args:
            df: Consolidated DataFrame with all years (Pandas or PySpark)
            engine: Optional engine type ('pandas' or 'pyspark'). Auto-detects if None.
        """
        self.engine = get_engine(engine)
        
        # Detect engine type from DataFrame
        if hasattr(df, 'toPandas'):
            # PySpark DataFrame
            self.is_spark = True
            self.df = df
            self.engine = get_engine('pyspark')
            row_count = self.engine.get_count(df)
        elif isinstance(df, pd.DataFrame):
            # Pandas DataFrame
            self.is_spark = False
            self.df = df.copy()
            self.engine = get_engine('pandas')
            row_count = len(df)
        else:
            raise ValueError(f"Unsupported DataFrame type: {type(df)}")
        
        self.analyses_results: Dict[str, Dict[str, Union[pd.DataFrame, SparkDataFrame]]] = {}
        logger.info(f"Initialized analyzer with {row_count:,} rows (engine: {self.engine.engine_type})")
    
    def analyze_volume_total(self) -> Dict[str, Union[pd.DataFrame, SparkDataFrame]]:
        """
        Analyze total volume of properties.
        
        Returns:
            Dictionary with total volume analysis results
        """
        logger.info("Analyzing total volume of properties")
        
        results = {}
        columns = self.engine.get_columns(self.df)
        
        if self.is_spark:
            from pyspark.sql import functions as F
            from pyspark.sql.functions import col, count, round
        
        # Total properties
            total_properties = self.engine.get_count(self.df)
            results["total_properties"] = self.engine.spark.createDataFrame(
                [("Total de Imóveis", total_properties)],
                ["metric", "value"]
            )
            
            # Volume by year (historical)
            volume_by_year = self.df.groupBy("ano do exercício").agg(
                count("*").alias("total_imoveis")
            ).withColumnRenamed("ano do exercício", "ano").orderBy("ano")
            results["volume_by_year"] = volume_by_year
            
            # Volume by type (tipo de uso do imóvel)
            if "tipo de uso do imóvel" in columns:
                volume_by_type = self.df.groupBy("tipo de uso do imóvel").agg(
                    count("*").alias("total_imoveis")
                ).withColumnRenamed("tipo de uso do imóvel", "tipo_uso") \
                 .withColumn("percentual", round(col("total_imoveis") / total_properties * 100, 2)) \
                 .orderBy(col("total_imoveis").desc())
                results["volume_by_type"] = volume_by_type
            
            # Volume by neighborhood (bairro)
            if "bairro" in columns:
                volume_by_neighborhood = self.df.groupBy("bairro").agg(
                    count("*").alias("total_imoveis")
                ).withColumn("percentual", round(col("total_imoveis") / total_properties * 100, 2)) \
                 .orderBy(col("total_imoveis").desc())
                results["volume_by_neighborhood"] = volume_by_neighborhood
            
            # Volume by year AND type (cross-analysis)
            if "tipo de uso do imóvel" in columns:
                volume_by_year_type = self.df.groupBy("ano do exercício", "tipo de uso do imóvel").agg(
                    count("*").alias("total_imoveis")
                ).withColumnRenamed("ano do exercício", "ano") \
                 .withColumnRenamed("tipo de uso do imóvel", "tipo_uso") \
                 .orderBy("ano", "tipo_uso")
                results["volume_by_year_type"] = volume_by_year_type
            
            # Volume by year AND neighborhood
            if "bairro" in columns:
                volume_by_year_neighborhood = self.df.groupBy("ano do exercício", "bairro").agg(
                    count("*").alias("total_imoveis")
                ).withColumnRenamed("ano do exercício", "ano") \
                 .orderBy("ano", col("total_imoveis").desc())
                results["volume_by_year_neighborhood"] = volume_by_year_neighborhood
        else:
            # Pandas implementation
            total_properties = len(self.df)
            results["total_properties"] = pd.DataFrame({
                "metric": ["Total de Imóveis"],
                "value": [total_properties]
            })
            
            # Volume by year (historical)
            volume_by_year = self.df.groupby("ano do exercício").size().reset_index()
            volume_by_year.columns = ["ano", "total_imoveis"]
            volume_by_year = volume_by_year.sort_values("ano")
            results["volume_by_year"] = volume_by_year
            
            # Volume by type (tipo de uso do imóvel)
            if "tipo de uso do imóvel" in columns:
                volume_by_type = self.df.groupby("tipo de uso do imóvel", observed=False).size().reset_index()
                volume_by_type.columns = ["tipo_uso", "total_imoveis"]
                volume_by_type = volume_by_type.sort_values("total_imoveis", ascending=False)
                volume_by_type["percentual"] = (
                    volume_by_type["total_imoveis"] / total_properties * 100
                ).round(2)
                results["volume_by_type"] = volume_by_type
            
            # Volume by neighborhood (bairro)
            if "bairro" in columns:
                volume_by_neighborhood = self.df.groupby("bairro").size().reset_index()
                volume_by_neighborhood.columns = ["bairro", "total_imoveis"]
                volume_by_neighborhood = volume_by_neighborhood.sort_values(
                    "total_imoveis", 
                    ascending=False
                )
                volume_by_neighborhood["percentual"] = (
                    volume_by_neighborhood["total_imoveis"] / total_properties * 100
                ).round(2)
                results["volume_by_neighborhood"] = volume_by_neighborhood
            
            # Volume by year AND type (cross-analysis)
            if "tipo de uso do imóvel" in columns:
                volume_by_year_type = self.df.groupby(
                    ["ano do exercício", "tipo de uso do imóvel"], observed=False
                ).size().reset_index()
                volume_by_year_type.columns = ["ano", "tipo_uso", "total_imoveis"]
                volume_by_year_type = volume_by_year_type.sort_values(["ano", "tipo_uso"])
                results["volume_by_year_type"] = volume_by_year_type
            
            # Volume by year AND neighborhood
            if "bairro" in columns:
                volume_by_year_neighborhood = self.df.groupby(
                    ["ano do exercício", "bairro"]
                ).size().reset_index()
                volume_by_year_neighborhood.columns = ["ano", "bairro", "total_imoveis"]
                volume_by_year_neighborhood = volume_by_year_neighborhood.sort_values(
                    ["ano", "total_imoveis"], 
                    ascending=[True, False]
                )
                results["volume_by_year_neighborhood"] = volume_by_year_neighborhood
        
        self.analyses_results["volume_analysis"] = results
        logger.info("[OK] Volume analysis complete")
        
        return results
    
    def analyze_distribution_physical(self) -> Dict[str, Union[pd.DataFrame, SparkDataFrame]]:
        """
        Analyze physical distribution of properties (type, neighborhood, etc.).
        
        Returns:
            Dictionary with distribution analysis results
        """
        logger.info("Analyzing physical distribution of properties")
        
        results = {}
        columns = self.engine.get_columns(self.df)
        total_count = self.engine.get_count(self.df)
        
        if self.is_spark:
            from pyspark.sql.functions import col, count, round, desc, lit
            
            # Distribution by type (tipo de uso do imóvel) - historical
            if "tipo de uso do imóvel" in columns:
                dist_by_type = self.df.groupBy("tipo de uso do imóvel").agg(
                    count("*").alias("quantidade")
                ).withColumnRenamed("tipo de uso do imóvel", "tipo_uso") \
                 .withColumn("percentual", round(col("quantidade") / total_count * 100, 2))
                results["distribution_by_type"] = dist_by_type
            
            # Distribution by neighborhood - historical (top 20)
            if "bairro" in columns:
                dist_by_neighborhood = self.df.groupBy("bairro").agg(
                    count("*").alias("quantidade")
                ).withColumn("percentual", round(col("quantidade") / total_count * 100, 2)) \
                 .orderBy(desc("quantidade")).limit(20)
                results["distribution_by_neighborhood_top20"] = dist_by_neighborhood
            
            # Distribution by year (temporal distribution)
            dist_by_year = self.df.groupBy("ano do exercício").agg(
                count("*").alias("quantidade")
            ).withColumnRenamed("ano do exercício", "ano") \
             .withColumn("percentual", round(col("quantidade") / total_count * 100, 2)) \
             .orderBy("ano")
            results["distribution_by_year"] = dist_by_year
            
            # Distribution by construction type
            if "Tipo de Construção" in columns:
                dist_by_construction = self.df.groupBy("Tipo de Construção").agg(
                    count("*").alias("quantidade")
                ).withColumnRenamed("Tipo de Construção", "tipo_construcao") \
                 .withColumn("percentual", round(col("quantidade") / total_count * 100, 2))
                results["distribution_by_construction"] = dist_by_construction
            
            # Distribution by neighborhood per year (detailed)
            if "bairro" in columns:
                # Get unique years
                years_df = self.df.select("ano do exercício").distinct().orderBy("ano do exercício")
                years = [row["ano do exercício"] for row in years_df.collect()]
                
                yearly_neighborhood_dfs = []
                for year in years:
                    year_data = self.df.filter(col("ano do exercício") == year)
                    year_dist = year_data.groupBy("bairro").agg(
                        count("*").alias("quantidade")
                    ).withColumn("ano", lit(year)).orderBy(desc("quantidade")).limit(10)
                    yearly_neighborhood_dfs.append(year_dist)
                
                if yearly_neighborhood_dfs:
                    from functools import reduce
                    results["top_neighborhoods_by_year"] = reduce(
                        lambda a, b: a.unionByName(b, allowMissingColumns=True),
                        yearly_neighborhood_dfs
                    )
        else:
            # Pandas implementation
        # Distribution by type (tipo de uso do imóvel) - historical
            if "tipo de uso do imóvel" in columns:
                dist_by_type = self.df["tipo de uso do imóvel"].value_counts().reset_index()
                dist_by_type.columns = ["tipo_uso", "quantidade"]
                dist_by_type["percentual"] = (
                        dist_by_type["quantidade"] / total_count * 100
                ).round(2)
                results["distribution_by_type"] = dist_by_type
        
        # Distribution by neighborhood - historical
            if "bairro" in columns:
                dist_by_neighborhood = self.df["bairro"].value_counts().head(20).reset_index()
                dist_by_neighborhood.columns = ["bairro", "quantidade"]
                dist_by_neighborhood["percentual"] = (
                        dist_by_neighborhood["quantidade"] / total_count * 100
                ).round(2)
                results["distribution_by_neighborhood_top20"] = dist_by_neighborhood
        
        # Distribution by year (temporal distribution)
        dist_by_year = self.df["ano do exercício"].value_counts().sort_index().reset_index()
        dist_by_year.columns = ["ano", "quantidade"]
        dist_by_year["percentual"] = (
                dist_by_year["quantidade"] / total_count * 100
        ).round(2)
        results["distribution_by_year"] = dist_by_year
        
        # Distribution by construction type
        if "Tipo de Construção" in columns:
            dist_by_construction = self.df["Tipo de Construção"].value_counts().reset_index()
            dist_by_construction.columns = ["tipo_construcao", "quantidade"]
            dist_by_construction["percentual"] = (
                    dist_by_construction["quantidade"] / total_count * 100
            ).round(2)
            results["distribution_by_construction"] = dist_by_construction
        
        # Distribution by neighborhood per year (detailed)
            if "bairro" in columns:
                yearly_neighborhood = []
                for year in sorted(self.df["ano do exercício"].unique()):
                    year_data = self.df[self.df["ano do exercício"] == year]
                    year_dist = year_data["bairro"].value_counts().head(10).reset_index()
                    year_dist.columns = ["bairro", "quantidade"]
                    year_dist["ano"] = year
                    yearly_neighborhood.append(year_dist)
            
            if yearly_neighborhood:
                results["top_neighborhoods_by_year"] = pd.concat(
                    yearly_neighborhood, 
                    ignore_index=True
                )
        
        self.analyses_results["distribution_analysis"] = results
        logger.info("[OK] Distribution analysis complete")
        
        return results
    
    def analyze_tax_value_trends(self) -> Dict[str, Union[pd.DataFrame, SparkDataFrame]]:
        """
        Analyze IPTU tax value trends over time.
        This is an additional analysis beyond the required volume analysis.
        
        Returns:
            Dictionary with tax value analysis results
        """
        logger.info("Analyzing tax value trends")
        
        results = {}
        columns = self.engine.get_columns(self.df)
        
        if self.is_spark:
            from pyspark.sql.functions import col, mean, min as spark_min, max as spark_max, sum as spark_sum, count, regexp_replace, trim
            from pyspark.sql import functions as F
            from pyspark.sql.types import DoubleType
            
            # Convert valor IPTU to numeric if possible
            if "valor IPTU" in columns:
                # Clean and convert valor IPTU
                df_temp = self.df.withColumn(
                    "valor_iptu_clean",
                    trim(regexp_replace(regexp_replace(col("valor IPTU"), ",", "."), "R\\$", ""))
                ).withColumn(
                    "valor_iptu_numeric",
                    col("valor_iptu_clean").cast(DoubleType())
                )
                
                tax_stats_by_year = df_temp.groupBy("ano do exercício").agg(
                    mean("valor_iptu_numeric").alias("media"),
                    F.expr("percentile_approx(valor_iptu_numeric, 0.5)").alias("mediana"),
                    spark_min("valor_iptu_numeric").alias("minimo"),
                    spark_max("valor_iptu_numeric").alias("maximo"),
                    spark_sum("valor_iptu_numeric").alias("total"),
                    count("*").alias("count")
                ).withColumnRenamed("ano do exercício", "ano")
                
                results["tax_stats_by_year"] = tax_stats_by_year
                
                # Average tax by neighborhood (top 20)
                if "bairro" in columns:
                    avg_tax_by_neighborhood = df_temp.groupBy("bairro").agg(
                        mean("valor_iptu_numeric").alias("media"),
                        spark_sum("valor_iptu_numeric").alias("total"),
                        count("*").alias("count")
                    ).orderBy(col("media").desc()).limit(20)
                    
                    results["avg_tax_by_neighborhood_top20"] = avg_tax_by_neighborhood
            
            # Analyze property value trends
            if "valor total do imóvel estimado" in columns:
                df_temp = self.df.withColumn(
                    "valor_imovel_clean",
                    trim(regexp_replace(regexp_replace(col("valor total do imóvel estimado"), ",", "."), "R\\$", ""))
                ).withColumn(
                    "valor_imovel_numeric",
                    col("valor_imovel_clean").cast(DoubleType())
                )
                
                property_value_by_year = df_temp.groupBy("ano do exercício").agg(
                    mean("valor_imovel_numeric").alias("media"),
                    F.expr("percentile_approx(valor_imovel_numeric, 0.5)").alias("mediana"),
                    spark_sum("valor_imovel_numeric").alias("total"),
                    count("*").alias("count")
                ).withColumnRenamed("ano do exercício", "ano")
                
                results["property_value_by_year"] = property_value_by_year
        else:
            # Pandas implementation
        # Convert valor IPTU to numeric if possible
            if "valor IPTU" in columns:
                # Convert to numeric (handling formatting)
                df_temp = self.df.copy()
                valor_iptu_clean = df_temp["valor IPTU"].astype(str).str.replace(
                    ',', '.', regex=False
                ).str.replace(
                    'R$', '', regex=False
                ).str.strip()
                valor_iptu_numeric = pd.to_numeric(valor_iptu_clean, errors='coerce')
                df_temp["valor_iptu_numeric"] = valor_iptu_numeric
            
            tax_stats_by_year = df_temp.groupby("ano do exercício")["valor_iptu_numeric"].agg([
                ('media', 'mean'),
                ('mediana', 'median'),
                ('minimo', 'min'),
                ('maximo', 'max'),
                ('total', 'sum'),
                ('count', 'count')
            ]).reset_index()
            
            results["tax_stats_by_year"] = tax_stats_by_year
            
            # Average tax by neighborhood (top 20)
            if "bairro" in columns:
                df_temp["bairro"] = self.df["bairro"]
                avg_tax_by_neighborhood = df_temp.groupby("bairro")["valor_iptu_numeric"].agg([
                    ('media', 'mean'),
                    ('total', 'sum'),
                    ('count', 'count')
                ]).reset_index()
                avg_tax_by_neighborhood = avg_tax_by_neighborhood.sort_values(
                    "media", 
                    ascending=False
                ).head(20)
                results["avg_tax_by_neighborhood_top20"] = avg_tax_by_neighborhood
        
        # Analyze property value trends
            if "valor total do imóvel estimado" in columns:
                # Similar conversion for property value
                df_temp = self.df.copy()
                valor_imovel_clean = self.df["valor total do imóvel estimado"].astype(str).str.replace(
                    ',', '.', regex=False
                ).str.replace('R$', '', regex=False).str.strip()
                valor_imovel_numeric = pd.to_numeric(valor_imovel_clean, errors='coerce')
                df_temp["valor_imovel_numeric"] = valor_imovel_numeric
            
            property_value_by_year = df_temp.groupby("ano do exercício")["valor_imovel_numeric"].agg([
                ('media', 'mean'),
                ('mediana', 'median'),
                ('total', 'sum'),
                ('count', 'count')
            ]).reset_index()
            
            results["property_value_by_year"] = property_value_by_year
        
        self.analyses_results["tax_value_analysis"] = results
        logger.info("[OK] Tax value trends analysis complete")
        
        return results
    
    def generate_all_analyses(self) -> Dict[str, Dict]:
        """
        Generate all available analyses.
        
        Returns:
            Dictionary with all analysis results
        """
        logger.info("Generating all analyses")
        
        all_results = {}
        
        # Required analyses
        all_results["volume"] = self.analyze_volume_total()
        all_results["distribution"] = self.analyze_distribution_physical()
        
        # Additional analysis
        all_results["tax_trends"] = self.analyze_tax_value_trends()
        
        logger.info("[OK] All analyses complete")
        return all_results
    
    def save_analyses(self, output_dir: Optional[Path] = None) -> None:
        """
        Save all analysis results to CSV files.
        
        Args:
            output_dir: Directory to save analysis files. Uses default if None.
        """
        output_dir = output_dir or ANALYSIS_OUTPUT_PATH
        output_dir.mkdir(exist_ok=True)
        
        logger.info(f"Saving analyses to {output_dir}")
        
        for analysis_name, analysis_results in self.analyses_results.items():
            analysis_dir = output_dir / analysis_name
            analysis_dir.mkdir(exist_ok=True)
            
            for result_name, result_df in analysis_results.items():
                    file_path = analysis_dir / f"{result_name}.csv"
                
            if self.is_spark and hasattr(result_df, 'toPandas'):
                # Convert PySpark DataFrame to Pandas for CSV export
                try:
                    pandas_df = result_df.toPandas()
                    pandas_df.to_csv(file_path, index=False)
                    logger.info(f"  Saved: {file_path} ({len(pandas_df)} rows)")
                except Exception as e:
                    logger.warning(f"  Failed to save {file_path}: {e}")
            elif isinstance(result_df, pd.DataFrame):
                result_df.to_csv(file_path, index=False)
                logger.info(f"  Saved: {file_path} ({len(result_df)} rows)")
            else:
                logger.warning(f"  Skipping {result_name}: unsupported DataFrame type")
        
        logger.info("[OK] All analyses saved")

