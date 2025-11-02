"""
Analysis module for IPTU pipeline.
Provides various analyses on the consolidated dataset.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional

from iptu_pipeline.config import ANALYSIS_OUTPUT_PATH
from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("analysis")


class IPTUAnalyzer:
    """Performs analyses on IPTU data."""
    
    def __init__(self, df: pd.DataFrame):
        """
        Initialize analyzer with consolidated dataset.
        
        Args:
            df: Consolidated DataFrame with all years
        """
        self.df = df.copy()
        self.analyses_results: Dict[str, pd.DataFrame] = {}
        logger.info(f"Initialized analyzer with {len(df):,} rows")
    
    def analyze_volume_total(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze total volume of properties.
        
        Returns:
            Dictionary with total volume analysis results
        """
        logger.info("Analyzing total volume of properties")
        
        results = {}
        
        # Total properties
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
        if "tipo de uso do imóvel" in self.df.columns:
            volume_by_type = self.df.groupby("tipo de uso do imóvel", observed=False).size().reset_index()
            volume_by_type.columns = ["tipo_uso", "total_imoveis"]
            volume_by_type = volume_by_type.sort_values("total_imoveis", ascending=False)
            volume_by_type["percentual"] = (
                volume_by_type["total_imoveis"] / total_properties * 100
            ).round(2)
            results["volume_by_type"] = volume_by_type
        
        # Volume by neighborhood (bairro)
        if "bairro" in self.df.columns:
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
        if "tipo de uso do imóvel" in self.df.columns:
            volume_by_year_type = self.df.groupby(
                ["ano do exercício", "tipo de uso do imóvel"], observed=False
            ).size().reset_index()
            volume_by_year_type.columns = ["ano", "tipo_uso", "total_imoveis"]
            volume_by_year_type = volume_by_year_type.sort_values(["ano", "tipo_uso"])
            results["volume_by_year_type"] = volume_by_year_type
        
        # Volume by year AND neighborhood
        if "bairro" in self.df.columns:
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
    
    def analyze_distribution_physical(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze physical distribution of properties (type, neighborhood, etc.).
        
        Returns:
            Dictionary with distribution analysis results
        """
        logger.info("Analyzing physical distribution of properties")
        
        results = {}
        
        # Distribution by type (tipo de uso do imóvel) - historical
        if "tipo de uso do imóvel" in self.df.columns:
            dist_by_type = self.df["tipo de uso do imóvel"].value_counts().reset_index()
            dist_by_type.columns = ["tipo_uso", "quantidade"]
            dist_by_type["percentual"] = (
                dist_by_type["quantidade"] / len(self.df) * 100
            ).round(2)
            results["distribution_by_type"] = dist_by_type
        
        # Distribution by neighborhood - historical
        if "bairro" in self.df.columns:
            dist_by_neighborhood = self.df["bairro"].value_counts().head(20).reset_index()
            dist_by_neighborhood.columns = ["bairro", "quantidade"]
            dist_by_neighborhood["percentual"] = (
                dist_by_neighborhood["quantidade"] / len(self.df) * 100
            ).round(2)
            results["distribution_by_neighborhood_top20"] = dist_by_neighborhood
        
        # Distribution by year (temporal distribution)
        dist_by_year = self.df["ano do exercício"].value_counts().sort_index().reset_index()
        dist_by_year.columns = ["ano", "quantidade"]
        dist_by_year["percentual"] = (
            dist_by_year["quantidade"] / len(self.df) * 100
        ).round(2)
        results["distribution_by_year"] = dist_by_year
        
        # Distribution by construction type
        if "Tipo de Construção" in self.df.columns:
            dist_by_construction = self.df["Tipo de Construção"].value_counts().reset_index()
            dist_by_construction.columns = ["tipo_construcao", "quantidade"]
            dist_by_construction["percentual"] = (
                dist_by_construction["quantidade"] / len(self.df) * 100
            ).round(2)
            results["distribution_by_construction"] = dist_by_construction
        
        # Distribution by neighborhood per year (detailed)
        if "bairro" in self.df.columns:
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
    
    def analyze_tax_value_trends(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze IPTU tax value trends over time.
        This is an additional analysis beyond the required volume analysis.
        
        Returns:
            Dictionary with tax value analysis results
        """
        logger.info("Analyzing tax value trends")
        
        results = {}
        
        # Convert valor IPTU to numeric if possible
        if "valor IPTU" in self.df.columns:
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
            if "bairro" in self.df.columns:
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
        if "valor total do imóvel estimado" in self.df.columns:
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
                if isinstance(result_df, pd.DataFrame):
                    file_path = analysis_dir / f"{result_name}.csv"
                    result_df.to_csv(file_path, index=False)
                    logger.info(f"  Saved: {file_path}")
        
        logger.info("[OK] All analyses saved")

