"""
Visualization module for IPTU analysis results.
Generates plots from analysis CSV files using matplotlib and seaborn.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, List
import warnings
warnings.filterwarnings('ignore')

try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

from iptu_pipeline.config import ANALYSIS_OUTPUT_PATH, PLOTS_OUTPUT_PATH
from iptu_pipeline.utils.logger import setup_logger

logger = setup_logger("visualizations")

# Set style
if MATPLOTLIB_AVAILABLE:
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (12, 6)
    plt.rcParams['font.size'] = 10


class IPTUVizualizer:
    """Creates visualizations from IPTU analysis results."""
    
    def __init__(self, analysis_path: Optional[Path] = None):
        """
        Initialize visualizer.
        
        Args:
            analysis_path: Path to analysis results directory. Uses default if None.
        """
        self.analysis_path = analysis_path or ANALYSIS_OUTPUT_PATH
        self.plots_output_path = PLOTS_OUTPUT_PATH
        self.plots_output_path.mkdir(parents=True, exist_ok=True)
        
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("Matplotlib not available. Some visualizations will be skipped.")
    
    def load_analysis_data(self, analysis_type: str, filename: str) -> pd.DataFrame:
        """Load analysis data from CSV file."""
        file_path = self.analysis_path / analysis_type / filename
        if not file_path.exists():
            # Only warn if analysis directory exists (analysis was run but file missing)
            # If directory doesn't exist, analysis was likely skipped (e.g., PySpark mode)
            if self.analysis_path.exists() and (self.analysis_path / analysis_type).exists():
            logger.warning(f"File not found: {file_path}")
            return pd.DataFrame()
        return pd.read_csv(file_path)
    
    def plot_volume_by_year(self, save: bool = True) -> Optional[plt.Figure]:
        """Plot total properties by year."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("volume_analysis", "volume_by_year.csv")
        if df.empty:
            return None
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        bars = ax.bar(df["ano"], df["total_imoveis"], color='steelblue', alpha=0.7, edgecolor='navy')
        ax.set_xlabel("Ano", fontsize=12, fontweight='bold')
        ax.set_ylabel("Total de Imóveis", fontsize=12, fontweight='bold')
        ax.set_title("Total de Imóveis por Ano (Histórico)", fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='y', alpha=0.3)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height):,}',
                   ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "volume_by_year.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot: {output_path}")
        
        return fig
    
    def plot_volume_by_type(self, top_n: int = 10, save: bool = True) -> Optional[plt.Figure]:
        """Plot distribution of properties by type."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("volume_analysis", "volume_by_type.csv")
        if df.empty:
            return None
        
        df_top = df.head(top_n)
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
        
        # Pie chart
        colors = sns.color_palette("Set3", len(df_top))
        wedges, texts, autotexts = ax1.pie(
            df_top["total_imoveis"], 
            labels=df_top["tipo_uso"],
            autopct='%1.1f%%',
            startangle=90,
            colors=colors
        )
        ax1.set_title(f"Distribuição por Tipo de Uso (Top {top_n})", fontsize=14, fontweight='bold')
        
        # Bar chart
        df_top = df_top.sort_values("total_imoveis", ascending=True)
        bars = ax2.barh(df_top["tipo_uso"], df_top["total_imoveis"], color='coral', alpha=0.7)
        ax2.set_xlabel("Total de Imóveis", fontsize=12, fontweight='bold')
        ax2.set_ylabel("Tipo de Uso", fontsize=12, fontweight='bold')
        ax2.set_title(f"Volume por Tipo de Uso (Top {top_n})", fontsize=14, fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)
        
        # Add value labels
        for bar in bars:
            width = bar.get_width()
            ax2.text(width, bar.get_y() + bar.get_height()/2.,
                   f'{int(width):,}',
                   ha='left', va='center', fontweight='bold')
        
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "volume_by_type.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot: {output_path}")
        
        return fig
    
    def plot_top_neighborhoods(self, top_n: int = 20, save: bool = True) -> Optional[plt.Figure]:
        """Plot top neighborhoods by volume."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("volume_analysis", "volume_by_neighborhood.csv")
        if df.empty:
            return None
        
        df_top = df.head(top_n).sort_values("total_imoveis", ascending=True)
        
        fig, ax = plt.subplots(figsize=(12, 10))
        
        bars = ax.barh(df_top["bairro"], df_top["total_imoveis"], color='mediumseagreen', alpha=0.7)
        ax.set_xlabel("Total de Imóveis", fontsize=12, fontweight='bold')
        ax.set_ylabel("Bairro", fontsize=12, fontweight='bold')
        ax.set_title(f"Top {top_n} Bairros por Volume de Imóveis", fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3)
        
        # Add value and percentage labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            pct = df_top.iloc[i]["percentual"]
            ax.text(width, bar.get_y() + bar.get_height()/2.,
                   f'{int(width):,} ({pct:.1f}%)',
                   ha='left', va='center', fontweight='bold')
        
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "top_neighborhoods.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot: {output_path}")
        
        return fig
    
    def plot_volume_by_year_and_type(self, save: bool = True) -> Optional[plt.Figure]:
        """Plot volume by year and type (stacked area chart)."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("volume_analysis", "volume_by_year_type.csv")
        if df.empty:
            return None
        
        # Pivot for stacked chart
        pivot_df = df.pivot(index="ano", columns="tipo_uso", values="total_imoveis").fillna(0)
        # Get top 5 types
        top_types = df.groupby("tipo_uso")["total_imoveis"].sum().nlargest(5).index
        pivot_df = pivot_df[top_types]
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Stacked area chart
        ax.stackplot(pivot_df.index, *[pivot_df[col] for col in pivot_df.columns],
                     labels=pivot_df.columns, alpha=0.7)
        
        ax.set_xlabel("Ano", fontsize=12, fontweight='bold')
        ax.set_ylabel("Total de Imóveis", fontsize=12, fontweight='bold')
        ax.set_title("Evolução do Volume de Imóveis por Tipo de Uso (Top 5)", 
                    fontsize=14, fontweight='bold', pad=20)
        ax.legend(loc='upper left', frameon=True, shadow=True)
        ax.grid(alpha=0.3)
        
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "volume_by_year_type.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot: {output_path}")
        
        return fig
    
    def plot_tax_trends(self, save: bool = True) -> Optional[plt.Figure]:
        """Plot IPTU tax value trends over time."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("tax_value_analysis", "tax_stats_by_year.csv")
        if df.empty:
            return None
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12))
        
        # Tax value trends
        ax1.plot(df["ano do exercício"], df["media"], marker='o', linewidth=2, 
                markersize=8, label='Média', color='steelblue')
        ax1.plot(df["ano do exercício"], df["mediana"], marker='s', linewidth=2, 
                markersize=8, label='Mediana', color='coral', linestyle='--')
        ax1.fill_between(df["ano do exercício"], df["minimo"], df["maximo"], 
                         alpha=0.2, label='Min-Max', color='lightblue')
        ax1.set_xlabel("Ano", fontsize=12, fontweight='bold')
        ax1.set_ylabel("Valor IPTU (R$)", fontsize=12, fontweight='bold')
        ax1.set_title("Tendência de Valores de IPTU por Ano", fontsize=14, fontweight='bold', pad=20)
        ax1.legend(frameon=True, shadow=True, loc='best')
        ax1.grid(alpha=0.3)
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x:,.0f}'))
        
        # Total tax by year
        bars = ax2.bar(df["ano do exercício"], df["total"], color='mediumseagreen', alpha=0.7)
        ax2.set_xlabel("Ano", fontsize=12, fontweight='bold')
        ax2.set_ylabel("Total Arrecadado (R$)", fontsize=12, fontweight='bold')
        ax2.set_title("Total de IPTU Arrecadado por Ano", fontsize=14, fontweight='bold', pad=20)
        ax2.grid(axis='y', alpha=0.3)
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x/1e9:.2f}B' if x >= 1e9 else f'R$ {x/1e6:.1f}M'))
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                   f'R${height/1e9:.2f}B' if height >= 1e9 else f'R${height/1e6:.1f}M',
                   ha='center', va='bottom', fontweight='bold', fontsize=9)
        
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "tax_trends.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot: {output_path}")
        
        return fig
    
    def plot_top_tax_neighborhoods(self, top_n: int = 20, save: bool = True) -> Optional[plt.Figure]:
        """Plot top neighborhoods by average tax value."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("tax_value_analysis", "avg_tax_by_neighborhood_top20.csv")
        if df.empty:
            return None
        
        df_top = df.head(top_n).sort_values("media", ascending=True)
        
        fig, ax = plt.subplots(figsize=(12, 10))
        
        bars = ax.barh(df_top["bairro"], df_top["media"], color='crimson', alpha=0.7)
        ax.set_xlabel("Valor Médio de IPTU (R$)", fontsize=12, fontweight='bold')
        ax.set_ylabel("Bairro", fontsize=12, fontweight='bold')
        ax.set_title(f"Top {top_n} Bairros por Valor Médio de IPTU", 
                    fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x:,.0f}'))
        
        # Add value labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            count = int(df_top.iloc[i]["count"])
            ax.text(width, bar.get_y() + bar.get_height()/2.,
                   f'R$ {width:,.0f} (n={count:,})',
                   ha='left', va='center', fontweight='bold')
        
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "top_tax_neighborhoods.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot: {output_path}")
        
        return fig
    
    def plot_distribution_by_construction(self, save: bool = True) -> Optional[plt.Figure]:
        """Plot distribution by construction type."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("distribution_analysis", "distribution_by_construction.csv")
        if df.empty:
            return None
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        df = df.sort_values("quantidade", ascending=False)
        bars = ax.bar(range(len(df)), df["quantidade"], color='purple', alpha=0.7)
        ax.set_xticks(range(len(df)))
        ax.set_xticklabels(df["tipo_construcao"], rotation=45, ha='right')
        ax.set_xlabel("Tipo de Construção", fontsize=12, fontweight='bold')
        ax.set_ylabel("Quantidade", fontsize=12, fontweight='bold')
        ax.set_title("Distribuição de Imóveis por Tipo de Construção", 
                    fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='y', alpha=0.3)
        
        # Add value labels
        for i, bar in enumerate(bars):
            height = bar.get_height()
            pct = df.iloc[i]["percentual"]
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height):,}\n({pct:.1f}%)',
                   ha='center', va='bottom', fontweight='bold', fontsize=9)
        
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "distribution_by_construction.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot: {output_path}")
        
        return fig
    
    def plot_temporal_distribution(self, save: bool = True) -> Optional[plt.Figure]:
        """Plot temporal distribution of properties."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("distribution_analysis", "distribution_by_year.csv")
        if df.empty:
            return None
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        ax.plot(df["ano"], df["quantidade"], marker='o', linewidth=3, 
               markersize=10, color='darkblue', markerfacecolor='lightblue',
               markeredgewidth=2, markeredgecolor='darkblue')
        ax.fill_between(df["ano"], df["quantidade"], alpha=0.3, color='lightblue')
        ax.set_xlabel("Ano", fontsize=12, fontweight='bold')
        ax.set_ylabel("Quantidade de Imóveis", fontsize=12, fontweight='bold')
        ax.set_title("Distribuição Temporal de Imóveis", fontsize=14, fontweight='bold', pad=20)
        ax.grid(alpha=0.3)
        
        # Add value labels
        for _, row in df.iterrows():
            ax.text(row["ano"], row["quantidade"],
                   f'{int(row["quantidade"]):,}\n({row["percentual"]:.1f}%)',
                   ha='center', va='bottom', fontweight='bold', fontsize=9)
        
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "temporal_distribution.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot: {output_path}")
        
        return fig
    
    def generate_all_plots(self, close_figs: bool = True) -> Dict[str, Optional[plt.Figure]]:
        """
        Generate all available plots.
        
        Args:
            close_figs: Whether to close figures after saving
        
        Returns:
            Dictionary mapping plot names to figure objects
        """
        logger.info("Generating all visualizations...")
        
        plots = {}
        
        try:
            plots["volume_by_year"] = self.plot_volume_by_year()
            plots["volume_by_type"] = self.plot_volume_by_type()
            plots["top_neighborhoods"] = self.plot_top_neighborhoods()
            plots["volume_by_year_type"] = self.plot_volume_by_year_and_type()
            plots["tax_trends"] = self.plot_tax_trends()
            plots["top_tax_neighborhoods"] = self.plot_top_tax_neighborhoods()
            plots["distribution_by_construction"] = self.plot_distribution_by_construction()
            plots["temporal_distribution"] = self.plot_temporal_distribution()
            
            if close_figs and MATPLOTLIB_AVAILABLE:
                for fig in plots.values():
                    if fig is not None:
                        plt.close(fig)
            
            logger.info(f"[OK] Generated {len([p for p in plots.values() if p is not None])} plots")
            
        except Exception as e:
            logger.error(f"Error generating plots: {str(e)}")
        
        return plots
    
    def create_summary_report_html(self) -> Path:
        """Create HTML report with all plots embedded."""
        plots = self.generate_all_plots(close_figs=False)
        
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>IPTU Analysis Visualizations</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
                .container { max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
                h1 { color: #2c3e50; text-align: center; }
                .plot-section { margin: 30px 0; border-bottom: 2px solid #ecf0f1; padding-bottom: 20px; }
                .plot-section:last-child { border-bottom: none; }
                img { max-width: 100%; height: auto; display: block; margin: 20px auto; }
                .plot-title { font-size: 18px; font-weight: bold; color: #34495e; margin-bottom: 10px; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>IPTU Data Analysis - Visualizations Report</h1>
        """
        
        plot_titles = {
            "volume_by_year": "Total de Imóveis por Ano",
            "volume_by_type": "Distribuição por Tipo de Uso",
            "top_neighborhoods": "Top Bairros por Volume",
            "volume_by_year_type": "Evolução por Ano e Tipo",
            "tax_trends": "Tendências de Valores de IPTU",
            "top_tax_neighborhoods": "Bairros com Maiores Valores de IPTU",
            "distribution_by_construction": "Distribuição por Tipo de Construção",
            "temporal_distribution": "Distribuição Temporal"
        }
        
        for plot_name, fig in plots.items():
            if fig is not None:
                plot_path = self.plots_output_path / f"{plot_name}.png"
                if plot_path.exists():
                    html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">{plot_titles.get(plot_name, plot_name)}</div>
                        <img src="{plot_path.name}" alt="{plot_name}">
                    </div>
                    """
        
        html_content += """
            </div>
        </body>
        </html>
        """
        
        output_path = self.plots_output_path / "visualizations_report.html"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"[OK] HTML report created: {output_path}")
        
        # Close all figures
        if MATPLOTLIB_AVAILABLE:
            plt.close('all')
        
        return output_path


def generate_plots_from_analysis_results(analysis_path: Optional[Path] = None) -> Dict[str, Path]:
    """
    Convenience function to generate all plots from analysis results.
    
    Args:
        analysis_path: Path to analysis results. Uses default if None.
    
    Returns:
        Dictionary with plot file paths
    """
    viz = IPTUVizualizer(analysis_path)
    viz.generate_all_plots()
    html_report = viz.create_summary_report_html()
    
    plot_files = {}
    for plot_file in viz.plots_output_path.glob("*.png"):
        plot_files[plot_file.stem] = plot_file
    
    plot_files["html_report"] = html_report
    
    return plot_files

