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
                ).filter(col("valor_iptu_numeric").isNotNull())  # Filter out null values
                
                # Calculate statistics for non-zero values (better distribution insights)
                df_temp_nonzero = df_temp.filter(col("valor_iptu_numeric") > 0)
                
                if df_temp_nonzero.count() > 0:
                    # Use non-zero values for mean, median, min, max
                    tax_stats_nonzero = df_temp_nonzero.groupBy("ano do exercício").agg(
                        mean("valor_iptu_numeric").alias("media"),
                        F.expr("percentile_approx(valor_iptu_numeric, 0.5)").alias("mediana"),
                        spark_min("valor_iptu_numeric").alias("minimo"),
                        spark_max("valor_iptu_numeric").alias("maximo")
                    )
                    
                    # Total and count should include zeros (all valid records)
                    tax_total_count = df_temp.groupBy("ano do exercício").agg(
                        spark_sum("valor_iptu_numeric").alias("total"),
                        count("*").alias("count"),
                        count(col("valor_iptu_numeric").cast("double")).alias("count_valid")
                    )
                    
                    # Join both aggregations
                    tax_stats_by_year = tax_stats_nonzero.join(
                        tax_total_count, 
                        on="ano do exercício", 
                        how="outer"
                    ).withColumnRenamed("ano do exercício", "ano")
                else:
                    # Fallback: if no non-zero values, use all valid values
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
                
                # Filter out invalid values (NaN, zero, or negative) before aggregation
                # But keep zeros for total calculation (they represent actual zero tax)
                df_temp_valid = df_temp[df_temp["valor_iptu_numeric"].notna()]
                
                # Calculate statistics with valid values only (excluding zeros for mean/median)
                # This gives better insights into the distribution of non-zero IPTU values
                df_temp_nonzero = df_temp_valid[df_temp_valid["valor_iptu_numeric"] > 0]
                
                if not df_temp_nonzero.empty:
                    # Use non-zero values for mean, median, min, max
                    tax_stats_nonzero = df_temp_nonzero.groupby("ano do exercício")["valor_iptu_numeric"].agg([
                        ('media', 'mean'),
                        ('mediana', 'median'),
                        ('minimo', 'min'),
                        ('maximo', 'max')
                    ]).reset_index()
                    
                    # Total and count should include zeros (all valid records)
                    tax_total_count = df_temp_valid.groupby("ano do exercício")["valor_iptu_numeric"].agg([
                        ('total', 'sum'),
                        ('count', 'count'),
                        ('count_nonzero', lambda x: (x > 0).sum())  # Count of non-zero values
                    ]).reset_index()
                    
                    # Merge both aggregations (left join to preserve all years with non-zero values)
                    tax_stats_by_year = tax_stats_nonzero.merge(tax_total_count, on="ano do exercício", how='outer')
                    
                    # Fill missing stats from non-zero aggregation with zeros (years with only zeros)
                    for col in ['media', 'mediana', 'minimo', 'maximo']:
                        if col in tax_stats_by_year.columns:
                            tax_stats_by_year[col] = tax_stats_by_year[col].fillna(0)
                else:
                    # Fallback: if no non-zero values, use all valid values (including zeros)
                    tax_stats_by_year = df_temp_valid.groupby("ano do exercício")["valor_iptu_numeric"].agg([
                        ('media', 'mean'),
                        ('mediana', 'median'),
                        ('minimo', 'min'),
                        ('maximo', 'max'),
                        ('total', 'sum'),
                        ('count', 'count'),
                        ('count_nonzero', lambda x: (x > 0).sum())
                    ]).reset_index()
                
                tax_stats_by_year = tax_stats_by_year.rename(columns={"ano do exercício": "ano"})
                
                # Ensure numeric columns are properly typed
                for col in ['media', 'mediana', 'minimo', 'maximo', 'total', 'count', 'count_nonzero']:
                    if col in tax_stats_by_year.columns:
                        tax_stats_by_year[col] = pd.to_numeric(tax_stats_by_year[col], errors='coerce')
                
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
                property_value_by_year = property_value_by_year.rename(columns={"ano do exercício": "ano"})
                
                results["property_value_by_year"] = property_value_by_year
        
        self.analyses_results["tax_value_analysis"] = results
        logger.info("[OK] Tax value trends analysis complete")
        
        return results
    
    def analyze_construction_age(self) -> Dict[str, Union[pd.DataFrame, SparkDataFrame]]:
        """
        Analyze distribution by construction age.
        Answers: Como o inventário está distribuido em termos de idade de construção?
        
        Returns:
            Dictionary with age distribution analysis results
        """
        logger.info("Analyzing construction age distribution")
        
        results = {}
        columns = self.engine.get_columns(self.df)
        current_year = pd.Timestamp.now().year
        
        if "ano da construção corrigido" not in columns:
            logger.warning("Column 'ano da construção corrigido' not found")
            return results
        
        if self.is_spark:
            from pyspark.sql import functions as F
            from pyspark.sql.functions import col, count, when, lit, round, desc, mean as spark_mean, min as spark_min, max as spark_max
            from pyspark.sql.types import IntegerType
            
            # Calculate age from construction year
            df_with_age = self.df.withColumn(
                "ano_construcao",
                col("ano da construção corrigido").cast(IntegerType())
            ).withColumn(
                "idade_construcao",
                lit(current_year) - col("ano_construcao")
            ).filter(
                col("ano_construcao").isNotNull() & 
                (col("ano_construcao") > 0) & 
                (col("ano_construcao") <= current_year)
            )
            
            # Distribution by age ranges
            df_with_age_ranges = df_with_age.withColumn(
                "faixa_idade",
                when(col("idade_construcao") < 10, "0-10 anos")
                .when(col("idade_construcao") < 20, "11-20 anos")
                .when(col("idade_construcao") < 30, "21-30 anos")
                .when(col("idade_construcao") < 40, "31-40 anos")
                .when(col("idade_construcao") < 50, "41-50 anos")
                .when(col("idade_construcao") < 60, "51-60 anos")
                .otherwise("60+ anos")
            )
            
            age_dist = df_with_age_ranges.groupBy("faixa_idade").agg(
                count("*").alias("quantidade")
            ).orderBy(desc("quantidade"))
            
            results["age_distribution_by_range"] = age_dist
            
            # Age statistics
            age_stats = df_with_age.agg(
                spark_mean("idade_construcao").alias("media"),
                F.expr("percentile_approx(idade_construcao, 0.5)").alias("mediana"),
                spark_min("idade_construcao").alias("minimo"),
                spark_max("idade_construcao").alias("maximo")
            )
            results["age_statistics"] = age_stats
        else:
            # Pandas implementation
            df_with_age = self.df.copy()
            df_with_age["ano_construcao"] = pd.to_numeric(
                df_with_age["ano da construção corrigido"], errors='coerce'
            )
            df_with_age["idade_construcao"] = current_year - df_with_age["ano_construcao"]
            
            # Filter valid ages
            df_with_age = df_with_age[
                df_with_age["ano_construcao"].notna() & 
                (df_with_age["ano_construcao"] > 0) & 
                (df_with_age["ano_construcao"] <= current_year)
            ]
            
            # Age ranges
            df_with_age["faixa_idade"] = pd.cut(
                df_with_age["idade_construcao"],
                bins=[-1, 10, 20, 30, 40, 50, 60, float('inf')],
                labels=["0-10 anos", "11-20 anos", "21-30 anos", 
                       "31-40 anos", "41-50 anos", "51-60 anos", "60+ anos"]
            )
            
            age_dist = df_with_age["faixa_idade"].value_counts().reset_index()
            age_dist.columns = ["faixa_idade", "quantidade"]
            age_dist["percentual"] = (age_dist["quantidade"] / len(df_with_age) * 100).round(2)
            age_dist = age_dist.sort_values("quantidade", ascending=False)
            results["age_distribution_by_range"] = age_dist
            
            # Age statistics
            age_stats = pd.DataFrame({
                "metrica": ["media", "mediana", "minimo", "maximo"],
                "valor": [
                    df_with_age["idade_construcao"].mean(),
                    df_with_age["idade_construcao"].median(),
                    df_with_age["idade_construcao"].min(),
                    df_with_age["idade_construcao"].max()
                ]
            })
            results["age_statistics"] = age_stats
        
        self.analyses_results["age_analysis"] = results
        logger.info("[OK] Construction age analysis complete")
        return results
    
    def analyze_age_value_relationship(self) -> Dict[str, Union[pd.DataFrame, SparkDataFrame]]:
        """
        Analyze relationship between construction age and property value.
        Answers: Há relação direta entre idade e valor?
        
        Returns:
            Dictionary with age-value relationship analysis
        """
        logger.info("Analyzing age-value relationship")
        
        results = {}
        columns = self.engine.get_columns(self.df)
        current_year = pd.Timestamp.now().year
        
        required_cols = ["ano da construção corrigido", "valor IPTU", "valor total do imóvel estimado"]
        missing_cols = [col for col in required_cols if col not in columns]
        
        if missing_cols:
            logger.warning(f"Missing columns for age-value analysis: {missing_cols}")
            return results
        
        if self.is_spark:
            from pyspark.sql.functions import col, avg, count, when, lit, round, regexp_replace, trim
            from pyspark.sql.types import IntegerType, DoubleType
            
            # Calculate age and clean values
            df_analysis = self.df.withColumn(
                "ano_construcao",
                col("ano da construção corrigido").cast(IntegerType())
            ).withColumn(
                "idade_construcao",
                lit(current_year) - col("ano_construcao")
            )
            
            # Clean valor IPTU
            valor_iptu_clean = regexp_replace(
                regexp_replace(col("valor IPTU"), ",", "."), "R\\$", ""
            )
            df_analysis = df_analysis.withColumn(
                "valor_iptu_numeric",
                trim(valor_iptu_clean).cast(DoubleType())
            ).filter(
                col("ano_construcao").isNotNull() & 
                (col("ano_construcao") > 0) & 
                col("valor_iptu_numeric").isNotNull() &
                (col("valor_iptu_numeric") > 0)
            )
            
            # Group by age ranges
            df_with_ranges = df_analysis.withColumn(
                "faixa_idade",
                when(col("idade_construcao") < 10, "0-10")
                .when(col("idade_construcao") < 20, "11-20")
                .when(col("idade_construcao") < 30, "21-30")
                .when(col("idade_construcao") < 40, "31-40")
                .when(col("idade_construcao") < 50, "41-50")
                .otherwise("50+")
            )
            
            age_value_stats = df_with_ranges.groupBy("faixa_idade").agg(
                avg("valor_iptu_numeric").alias("valor_medio"),
                count("*").alias("quantidade")
            ).orderBy("faixa_idade")
            
            results["age_value_relationship"] = age_value_stats
        else:
            # Pandas implementation
            df_analysis = self.df.copy()
            df_analysis["ano_construcao"] = pd.to_numeric(
                df_analysis["ano da construção corrigido"], errors='coerce'
            )
            df_analysis["idade_construcao"] = current_year - df_analysis["ano_construcao"]
            
            # Clean valor IPTU
            valor_iptu_clean = df_analysis["valor IPTU"].astype(str).str.replace(
                ',', '.', regex=False
            ).str.replace('R$', '', regex=False).str.strip()
            df_analysis["valor_iptu_numeric"] = pd.to_numeric(valor_iptu_clean, errors='coerce')
            
            # Filter valid data
            df_analysis = df_analysis[
                df_analysis["ano_construcao"].notna() & 
                (df_analysis["ano_construcao"] > 0) &
                df_analysis["valor_iptu_numeric"].notna() &
                (df_analysis["valor_iptu_numeric"] > 0)
            ]
            
            # Age ranges
            df_analysis["faixa_idade"] = pd.cut(
                df_analysis["idade_construcao"],
                bins=[-1, 10, 20, 30, 40, 50, float('inf')],
                labels=["0-10", "11-20", "21-30", "31-40", "41-50", "50+"]
            )
            
            age_value_stats = df_analysis.groupby("faixa_idade").agg({
                "valor_iptu_numeric": ["mean", "count"]
            }).reset_index()
            age_value_stats.columns = ["faixa_idade", "valor_medio", "quantidade"]
            age_value_stats = age_value_stats.sort_values("faixa_idade")
            
            results["age_value_relationship"] = age_value_stats
        
        self.analyses_results["age_value_analysis"] = results
        logger.info("[OK] Age-value relationship analysis complete")
        return results
    
    def analyze_neighborhood_evolution(self) -> Dict[str, Union[pd.DataFrame, SparkDataFrame]]:
        """
        Analyze neighborhood evolution in terms of number of properties and values.
        Answers: Quais bairros apresentam maior evolução em número de imóveis? E em relação a valor?
        
        Returns:
            Dictionary with neighborhood evolution analysis
        """
        logger.info("Analyzing neighborhood evolution")
        
        results = {}
        columns = self.engine.get_columns(self.df)
        
        if "bairro" not in columns or "ano do exercício" not in columns:
            logger.warning("Required columns not found for neighborhood evolution")
            return results
        
        if self.is_spark:
            from pyspark.sql.functions import col, count, sum, avg, first, last, when, desc, regexp_replace, trim
            from pyspark.sql.window import Window
            from pyspark.sql.types import DoubleType
            
            # Clean valor IPTU
            valor_iptu_clean = regexp_replace(
                regexp_replace(col("valor IPTU"), ",", "."), "R\\$", ""
            )
            df_analysis = self.df.withColumn(
                "valor_iptu_numeric",
                trim(valor_iptu_clean).cast(DoubleType())
            ).filter(
                col("valor_iptu_numeric").isNotNull() & (col("valor_iptu_numeric") > 0)
            )
            
            # Get years
            years_df = df_analysis.select("ano do exercício").distinct().orderBy("ano do exercício")
            years = [row["ano do exercício"] for row in years_df.collect()]
            
            # Filter to 2020-2023 only (exclude 2024 if present)
            years_filtered = [y for y in years if 2020 <= y <= 2023]
            if len(years_filtered) < 2:
                logger.warning("Need at least 2 years in range 2020-2023 for evolution analysis")
                return results
            
            first_year = years_filtered[0]  # Should be 2020
            last_year = years_filtered[-1]   # Should be 2023
            
            # Count and average value by neighborhood and year
            neighborhood_year_stats = df_analysis.groupBy("bairro", "ano do exercício").agg(
                count("*").alias("quantidade"),
                avg("valor_iptu_numeric").alias("valor_medio")
            )
            
            # Calculate first and last year stats per neighborhood
            first_year_stats = neighborhood_year_stats.filter(
                col("ano do exercício") == first_year
            ).select(
                col("bairro").alias("bairro"),
                col("quantidade").alias("quantidade_inicial"),
                col("valor_medio").alias("valor_medio_inicial")
            )
            
            last_year_stats = neighborhood_year_stats.filter(
                col("ano do exercício") == last_year
            ).select(
                col("bairro").alias("bairro"),
                col("quantidade").alias("quantidade_final"),
                col("valor_medio").alias("valor_medio_final")
            )
            
            # Join and calculate evolution
            evolution = first_year_stats.join(
                last_year_stats, on="bairro", how="inner"
            ).withColumn(
                "crescimento_quantidade",
                ((col("quantidade_final") - col("quantidade_inicial")) / col("quantidade_inicial") * 100)
            ).withColumn(
                "crescimento_valor",
                ((col("valor_medio_final") - col("valor_medio_inicial")) / col("valor_medio_inicial") * 100)
            ).filter(
                col("quantidade_inicial") > 10  # Minimum threshold
            ).orderBy(desc("crescimento_quantidade"))
            
            results["neighborhood_evolution"] = evolution
            
            # Top growing neighborhoods by quantity
            results["top_growth_quantity"] = evolution.orderBy(desc("crescimento_quantidade")).limit(20)
            
            # Top growing neighborhoods by value
            results["top_growth_value"] = evolution.orderBy(desc("crescimento_valor")).limit(20)
        else:
            # Pandas implementation
            # Clean valor IPTU
            valor_iptu_clean = self.df["valor IPTU"].astype(str).str.replace(
                ',', '.', regex=False
            ).str.replace('R$', '', regex=False).str.strip()
            self.df["valor_iptu_numeric"] = pd.to_numeric(valor_iptu_clean, errors='coerce')
            
            df_analysis = self.df[
                self.df["valor_iptu_numeric"].notna() & 
                (self.df["valor_iptu_numeric"] > 0)
            ]
            
            years = sorted(df_analysis["ano do exercício"].unique())
            
            if len(years) < 2:
                logger.warning("Need at least 2 years for evolution analysis")
                return results
            
            # Filter to 2020-2023 only (exclude 2024 if present)
            years_filtered = [y for y in years if 2020 <= y <= 2023]
            if len(years_filtered) < 2:
                logger.warning("Need at least 2 years in range 2020-2023 for evolution analysis")
                return results
            
            first_year = years_filtered[0]  # Should be 2020
            last_year = years_filtered[-1]   # Should be 2023
            
            # Stats by neighborhood and year
            neighborhood_year_stats = df_analysis.groupby(["bairro", "ano do exercício"]).agg({
                "valor_iptu_numeric": ["count", "mean"]
            }).reset_index()
            neighborhood_year_stats.columns = ["bairro", "ano", "quantidade", "valor_medio"]
            
            # First and last year
            first_year_stats = neighborhood_year_stats[
                neighborhood_year_stats["ano"] == first_year
            ][["bairro", "quantidade", "valor_medio"]].rename(
                columns={"quantidade": "quantidade_inicial", "valor_medio": "valor_medio_inicial"}
            )
            
            last_year_stats = neighborhood_year_stats[
                neighborhood_year_stats["ano"] == last_year
            ][["bairro", "quantidade", "valor_medio"]].rename(
                columns={"quantidade": "quantidade_final", "valor_medio": "valor_medio_final"}
            )
            
            # Join and calculate evolution
            evolution = first_year_stats.merge(
                last_year_stats, on="bairro", how="inner"
            )
            evolution["crescimento_quantidade"] = (
                (evolution["quantidade_final"] - evolution["quantidade_inicial"]) / 
                evolution["quantidade_inicial"] * 100
            )
            evolution["crescimento_valor"] = (
                (evolution["valor_medio_final"] - evolution["valor_medio_inicial"]) / 
                evolution["valor_medio_inicial"] * 100
            )
            evolution = evolution[evolution["quantidade_inicial"] > 10]  # Minimum threshold
            evolution = evolution.sort_values("crescimento_quantidade", ascending=False)
            
            results["neighborhood_evolution"] = evolution
            results["top_growth_quantity"] = evolution.head(20)
            results["top_growth_value"] = evolution.sort_values("crescimento_valor", ascending=False).head(20)
        
        self.analyses_results["evolution_analysis"] = results
        logger.info("[OK] Neighborhood evolution analysis complete")
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
        
        # Additional analyses
        all_results["tax_trends"] = self.analyze_tax_value_trends()
        all_results["age"] = self.analyze_construction_age()
        all_results["age_value"] = self.analyze_age_value_relationship()
        all_results["evolution"] = self.analyze_neighborhood_evolution()
        
        logger.info("[OK] All analyses complete")
        return all_results
    
    def save_analyses(self, output_dir: Optional[Path] = None) -> None:
        """
        Save all analysis results to CSV and Parquet files.
        
        Args:
            output_dir: Directory to save CSV files. Uses default if None (gold_csv_dir).
        """
        from iptu_pipeline.config import settings
        
        # CSV files go to gold_csv_dir
        csv_output_dir = output_dir or settings.gold_csv_dir
        csv_output_dir.mkdir(exist_ok=True)
        
        # Parquet files go to gold_parquet_dir/analyses
        parquet_output_dir = settings.gold_parquet_dir / "analyses"
        parquet_output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Saving analyses CSV to {csv_output_dir}")
        logger.info(f"Saving analyses Parquet to {parquet_output_dir}")
        
        for analysis_name, analysis_results in self.analyses_results.items():
            csv_analysis_dir = csv_output_dir / analysis_name
            csv_analysis_dir.mkdir(exist_ok=True)
            
            parquet_analysis_dir = parquet_output_dir / analysis_name
            parquet_analysis_dir.mkdir(exist_ok=True)
            
            for result_name, result_df in analysis_results.items():
                csv_path = csv_analysis_dir / f"{result_name}.csv"
                parquet_path = parquet_analysis_dir / f"{result_name}.parquet"
                
                if self.is_spark and hasattr(result_df, 'toPandas'):
                    # Convert PySpark DataFrame to Pandas for export
                    # Handle DateType columns (PySpark DateType cannot be directly converted to Pandas)
                    try:
                        from pyspark.sql.types import DateType
                        from pyspark.sql import functions as F
                        
                        # Check for DateType columns and convert them to TimestampType
                        date_columns = [
                            field.name for field in result_df.schema.fields 
                            if isinstance(field.dataType, DateType)
                        ]
                        
                        if date_columns:
                            logger.debug(f"Converting DateType columns to TimestampType: {date_columns}")
                            for col_name in date_columns:
                                result_df = result_df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
                        
                        pandas_df = result_df.toPandas()
                        # Save as CSV (for backward compatibility)
                        pandas_df.to_csv(csv_path, index=False)
                        logger.info(f"  Saved CSV: {csv_path} ({len(pandas_df)} rows)")
                        # Save as Parquet (for gold layer)
                        pandas_df.to_parquet(parquet_path, index=False, engine='pyarrow')
                        logger.info(f"  Saved Parquet: {parquet_path} ({len(pandas_df)} rows)")
                    except Exception as e:
                        logger.warning(f"  Failed to save {result_name}: {e}")
                elif isinstance(result_df, pd.DataFrame):
                    # Save as CSV (for backward compatibility)
                    result_df.to_csv(csv_path, index=False)
                    logger.info(f"  Saved CSV: {csv_path} ({len(result_df)} rows)")
                    # Save as Parquet (for gold layer)
                    try:
                        result_df.to_parquet(parquet_path, index=False, engine='pyarrow')
                        logger.info(f"  Saved Parquet: {parquet_path} ({len(result_df)} rows)")
                    except Exception as e:
                        logger.warning(f"  Failed to save Parquet for {result_name}: {e}")
                else:
                    logger.warning(f"  Skipping {result_name}: unsupported DataFrame type")
        
        logger.info("[OK] All analyses saved")

