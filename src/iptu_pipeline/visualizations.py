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
        
        # Pie chart with direct labels
        colors = sns.color_palette("Set3", len(df_top))
        wedges, texts, autotexts = ax1.pie(
            df_top["total_imoveis"], 
            labels=df_top["tipo_uso"],  # Direct labels on pie slices
            autopct='%1.1f%%',
            startangle=90,
            colors=colors,
            textprops={'fontsize': 8}
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
        """Plot IPTU tax value trends as boxplot by year."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        # Load raw data from silver layer for boxplot
        from iptu_pipeline.config import SILVER_DIR
        import pandas as pd
        import numpy as np
        
        silver_path = SILVER_DIR / "iptu_silver_consolidated" / "data.parquet"
        
        # Load from silver layer (silver should always have correct column names)
        if not silver_path.exists():
            logger.error(f"Silver data not found: {silver_path}. Please run the pipeline to generate silver layer.")
            return None
        
        try:
            df = pd.read_parquet(silver_path)
            
            # Check if DataFrame is empty
            if df.empty:
                logger.error(f"Silver layer file exists but is EMPTY: {silver_path}")
                logger.error(f"The silver layer consolidation may have failed. Please run the pipeline again.")
                return None
            
            # Check if no columns (shouldn't happen but check anyway)
            if len(df.columns) == 0:
                logger.error(f"Silver layer file exists but has NO COLUMNS: {silver_path}")
                logger.error(f"The silver layer file may be corrupted. Please run the pipeline again.")
                return None
            
            logger.info(f"Loaded silver data: {len(df):,} rows, {len(df.columns)} columns")
            
            # Check if columns are corrupted (UUID column names)
            if len(df.columns) > 0 and df.columns[0].startswith('col-') and len(df.columns[0]) > 40:
                logger.error(f"Silver layer has corrupted column names (UUID). Please regenerate the silver layer.")
                logger.error(f"This indicates a problem during silver layer consolidation. Run the pipeline again.")
                return None
            
            # Check for valor IPTU column with better diagnostics
            if "valor IPTU" not in df.columns:
                all_cols = list(df.columns)
                similar_cols = [c for c in all_cols if 'valor' in str(c).lower() or 'iptu' in str(c).lower()]
                logger.error(f"Column 'valor IPTU' not found in silver layer.")
                logger.error(f"Available columns ({len(all_cols)}): {all_cols[:20]}{'...' if len(all_cols) > 20 else ''}")
                if similar_cols:
                    logger.error(f"Similar columns found: {similar_cols}")
                logger.error(f"Please regenerate the silver layer by running the pipeline.")
                return None
        except Exception as e:
            logger.error(f"Could not load silver data from {silver_path}: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
        
        valor_iptu_clean = df["valor IPTU"].astype(str).str.replace(
            ',', '.', regex=False
        ).str.replace('R$', '', regex=False).str.strip()
        df["valor_iptu_numeric"] = pd.to_numeric(valor_iptu_clean, errors='coerce')
        
        # Filter only positive values for boxplot
        df = df[df["valor_iptu_numeric"].notna() & (df["valor_iptu_numeric"] > 0)]
        
        # Get year column
        if "ano do exercício" in df.columns:
            df["ano"] = pd.to_numeric(df["ano do exercício"], errors='coerce')
        elif "ano" in df.columns:
            df["ano"] = pd.to_numeric(df["ano"], errors='coerce')
        else:
            logger.warning("No year column found")
            return None
        
        df = df.dropna(subset=["ano", "valor_iptu_numeric"])
        
        if df.empty:
            logger.warning("No valid data for boxplot")
            return None
        
        # Prepare boxplot data (one array per year)
        years = sorted(df["ano"].unique())
        box_data = [df[df["ano"] == year]["valor_iptu_numeric"].values for year in years]
        
        if not box_data or all(len(data) == 0 for data in box_data):
            logger.warning("No data to plot")
            return None
        
        # Create figure with boxplot
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Filter extreme outliers using IQR method before creating boxplot
        filtered_box_data = []
        for year_data in box_data:
            if len(year_data) == 0:
                filtered_box_data.append(year_data)
                continue
            
            # Calculate IQR
            q1 = np.percentile(year_data, 25)
            q3 = np.percentile(year_data, 75)
            iqr = q3 - q1
            
            # Filter outliers (values beyond 3*IQR from Q1/Q3)
            lower_bound = q1 - 3 * iqr
            upper_bound = q3 + 3 * iqr
            
            filtered_data = year_data[(year_data >= lower_bound) & (year_data <= upper_bound)]
            filtered_box_data.append(filtered_data)
        
        # Create boxplot with filtered data (whiskers at 1st and 99th percentile)
        bp = ax.boxplot(filtered_box_data, labels=[int(y) for y in years], 
                       patch_artist=True, showmeans=True,
                       meanline=False, whis=[1, 99])
        
        # Customize colors
        colors = sns.color_palette("Set2", len(bp['boxes']))
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
            patch.set_edgecolor('darkblue')
            patch.set_linewidth(1.5)
        
        # Customize other elements
        for element in ['whiskers', 'fliers', 'means', 'medians', 'caps']:
            if element in bp:
                for item in bp[element]:
                    item.set_color('darkblue')
                    item.set_linewidth(1.5)
        
        # Use log scale if data has large range
        max_val = df["valor_iptu_numeric"].max()
        median_val = df["valor_iptu_numeric"].median()
        if max_val > median_val * 50:
            ax.set_yscale('log')
            ax.yaxis.set_major_formatter(plt.FuncFormatter(
                lambda x, p: f'R${x/1e3:.0f}k' if x < 1e6 else f'R${x/1e6:.1f}M'
            ))
        else:
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x:,.0f}'))
        
        ax.set_xlabel("Ano", fontsize=12, fontweight='bold')
        ax.set_ylabel("Valor IPTU (R$)", fontsize=12, fontweight='bold')
        ax.set_title("Distribuição de Valores de IPTU por Ano (Boxplot)\n(Apenas valores não-zero)", 
                     fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='y', alpha=0.3)
        
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
        
        fig, ax = plt.subplots(figsize=(14, 7))
        
        df = df.sort_values("quantidade", ascending=False)
        bars = ax.bar(range(len(df)), df["quantidade"], color='purple', alpha=0.7, edgecolor='darkblue', linewidth=1.5)
        ax.set_xticks(range(len(df)))
        labels = ax.set_xticklabels(df["tipo_construcao"], rotation=45, fontsize=9)
        # Adjust label alignment
        for label in labels:
            label.set_ha('right')
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
                   ha='center', va='bottom', fontweight='bold', fontsize=8)
        
        # Adjust layout to prevent label cutoff
        plt.subplots_adjust(bottom=0.25, top=0.92)
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
    
    def plot_age_distribution(self, save: bool = True) -> Optional[plt.Figure]:
        """Plot distribution of properties by construction age ranges."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("age_analysis", "age_distribution_by_range.csv")
        if df.empty:
            return None
        
        # Calculate percentages
        total = df["quantidade"].sum()
        df["percentual"] = (df["quantidade"] / total * 100).round(2)
        
        # Sort by age range order (custom order)
        age_order = ["0-10 anos", "11-20 anos", "21-30 anos", "31-40 anos", 
                     "41-50 anos", "51-60 anos", "60+ anos"]
        df["order"] = df["faixa_idade"].apply(lambda x: age_order.index(x) if x in age_order else 999)
        df = df.sort_values("order")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
        
        # Bar chart
        colors = sns.color_palette("viridis", len(df))
        bars = ax1.bar(df["faixa_idade"], df["quantidade"], color=colors, alpha=0.7, edgecolor='darkblue', linewidth=1.5)
        ax1.set_xlabel("Faixa de Idade", fontsize=12, fontweight='bold')
        ax1.set_ylabel("Quantidade de Imóveis", fontsize=12, fontweight='bold')
        ax1.set_title("Distribuição por Faixas de Idade de Construção", fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)
        ax1.tick_params(axis='x', labelsize=9)
        # Rotate and align labels
        for label in ax1.get_xticklabels():
            label.set_rotation(45)
            label.set_ha('right')
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height):,}',
                   ha='center', va='bottom', fontweight='bold', fontsize=9)
        
        # Pie chart with direct labels
        wedges, texts, autotexts = ax2.pie(df["quantidade"], labels=df["faixa_idade"], autopct='%1.1f%%',
               startangle=90, colors=colors, textprops={'fontsize': 8})
        ax2.set_title("Distribuição Percentual por Idade", fontsize=14, fontweight='bold')
        
        # Adjust layout to prevent label cutoff
        plt.subplots_adjust(bottom=0.15, right=0.85)
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "age_distribution.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot: {output_path}")
        
        return fig
    
    def plot_age_value_relationship(self, save: bool = True) -> Optional[plt.Figure]:
        """Plot relationship between construction age and IPTU value."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("age_value_analysis", "age_value_relationship.csv")
        if df.empty:
            return None
        
        # Sort by age range order
        age_order = ["0-10", "11-20", "21-30", "31-40", "41-50", "50+"]
        df["order"] = df["faixa_idade"].apply(lambda x: age_order.index(x) if x in age_order else 999)
        df = df.sort_values("order")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
        
        # Bar chart - Average value by age
        colors = sns.color_palette("coolwarm", len(df))
        bars = ax1.bar(df["faixa_idade"], df["valor_medio"], color=colors, alpha=0.7, edgecolor='darkblue', linewidth=1.5)
        ax1.set_xlabel("Faixa de Idade (anos)", fontsize=12, fontweight='bold')
        ax1.set_ylabel("Valor Médio de IPTU (R$)", fontsize=12, fontweight='bold')
        ax1.set_title("Valor Médio de IPTU por Faixa de Idade", fontsize=14, fontweight='bold', pad=15)
        ax1.grid(axis='y', alpha=0.3)
        # Rotate and align labels
        for label in ax1.get_xticklabels():
            label.set_rotation(45)
            label.set_ha('right')
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x:,.0f}'))
        
        # Add value labels with better positioning
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                   f'R$ {height:,.0f}',
                   ha='center', va='bottom', fontweight='bold', fontsize=9,
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8, edgecolor='gray'))
        
        # Scatter plot - Value vs Quantity with proper scaling and legend
        # Normalize sizes for better visualization
        size_normalized = (df["quantidade"] / df["quantidade"].max() * 500) + 100
        
        scatter = ax2.scatter(df["quantidade"], df["valor_medio"], 
                            s=size_normalized, c=range(len(df)), 
                            cmap='viridis', alpha=0.7, edgecolors='darkblue', 
                            linewidth=2, label='Faixas de Idade')
        
        ax2.set_xlabel("Quantidade de Imóveis", fontsize=12, fontweight='bold')
        ax2.set_ylabel("Valor Médio de IPTU (R$)", fontsize=12, fontweight='bold')
        ax2.set_title("Relação: Quantidade vs Valor Médio\n(Tamanho = Quantidade)", 
                     fontsize=14, fontweight='bold', pad=15)
        ax2.grid(alpha=0.3, linestyle='--')
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x:,.0f}'))
        ax2.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
        
        # Add colorbar for age ranges
        cbar = plt.colorbar(scatter, ax=ax2, pad=0.02)
        cbar.set_label('Ordem de Faixa de Idade', fontsize=10, fontweight='bold')
        cbar.set_ticks(range(len(df)))
        cbar.set_ticklabels(df["faixa_idade"].values)
        
        # Add legend for bubble size (quantity)
        # Create proxy artists for size legend
        min_qty = df["quantidade"].min()
        max_qty = df["quantidade"].max()
        size_legend_elements = [
            plt.scatter([], [], s=(min_qty/max_qty * 500) + 100, c='gray', alpha=0.7, edgecolors='black', 
                       label=f'Quantidade: {min_qty:,.0f}'),
            plt.scatter([], [], s=(max_qty/max_qty * 500) + 100, c='gray', alpha=0.7, edgecolors='black',
                       label=f'Quantidade: {max_qty:,.0f}')
        ]
        ax2.legend(handles=size_legend_elements, loc='upper right', frameon=True, 
                  shadow=True, title='Tamanho da Bolha', title_fontsize=9, fontsize=8)
        
        # Add labels for each point with better positioning
        for _, row in df.iterrows():
            # Adjust annotation position based on quadrant
            offset_x = (row["quantidade"] * 0.02)
            offset_y = (row["valor_medio"] * 0.02)
            ax2.annotate(row["faixa_idade"], 
                        (row["quantidade"], row["valor_medio"]),
                        xytext=(offset_x, offset_y), textcoords='offset points', 
                        fontsize=9, fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7, edgecolor='black'))
        
        # Adjust layout to optimize space usage and prevent label cutoff
        plt.subplots_adjust(bottom=0.12, left=0.10, right=0.92, top=0.90, wspace=0.25)
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "age_value_relationship.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.2)
            logger.info(f"Saved plot: {output_path}")
        
        return fig
    
    def plot_neighborhood_growth_quantity(self, top_n: int = 15, save: bool = True) -> Optional[plt.Figure]:
        """Plot top neighborhoods by quantity growth."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("evolution_analysis", "top_growth_quantity.csv")
        if df.empty:
            return None
        
        # Filter to show only positive growth (real growth, not decline)
        # Only include neighborhoods with actual growth (positive values)
        df_positive = df[df["crescimento_quantidade"] > 0].sort_values("crescimento_quantidade", ascending=False)
        
        if len(df_positive) == 0:
            logger.warning("No positive growth found. Showing neighborhoods with smallest decline.")
            # If no positive growth, show least negative (smallest decline)
            df_negative = df[df["crescimento_quantidade"] <= 0].sort_values("crescimento_quantidade", ascending=True)
            df_top = df_negative.head(top_n)
        else:
            # Take top N positive growth values
            df_top = df_positive.head(top_n)
        
        # For visualization, sort ascending so bars go from smallest to largest (top to bottom)
        df_top = df_top.sort_values("crescimento_quantidade", ascending=True)
        
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Use different colors for positive/negative growth
        colors = ['crimson' if x < 0 else 'mediumseagreen' for x in df_top["crescimento_quantidade"]]
        
        bars = ax.barh(df_top["bairro"], df_top["crescimento_quantidade"], color=colors, alpha=0.7, edgecolor='darkblue', linewidth=1.5)
        ax.set_xlabel("Crescimento em Quantidade (%)", fontsize=12, fontweight='bold')
        ax.set_ylabel("Bairro", fontsize=12, fontweight='bold')
        ax.set_title(f"Top {top_n} Bairros com Maior Crescimento em Número de Imóveis", 
                    fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3)
        ax.axvline(x=0, color='black', linestyle='--', linewidth=1)
        
        # Calculate x-axis limits with padding for labels
        x_min = df_top["crescimento_quantidade"].min()
        x_max = df_top["crescimento_quantidade"].max()
        x_range = x_max - x_min
        padding_right = max(abs(x_range * 0.20), 5)  # 20% padding on right
        padding_left = max(abs(x_range * 0.25), 10)   # 25% padding on left (more for negative values)
        
        # Set x-axis limits with padding
        ax.set_xlim(x_min - padding_left, x_max + padding_right)
        
        # Add value labels positioned to avoid collision with y-axis
        for i, bar in enumerate(bars):
            width = bar.get_width()
            initial = int(df_top.iloc[i]["quantidade_inicial"])
            final = int(df_top.iloc[i]["quantidade_final"])
            
            # Position labels on the bars (not at the edge to avoid collision with y-axis)
            if width > 0:
                # Positive growth: label on the right edge of the bar
                label_x = width + (x_max + padding_right - width) * 0.02
                ha_pos = 'left'
            else:
                # Negative growth: label on the right edge of the bar (for negative bars)
                # Position at a safe distance from y-axis (using bar end + small offset)
                label_x = width + abs(x_min - padding_left - width) * 0.15
                ha_pos = 'left'  # Always left-aligned to avoid collision
            
            # Shorten label text to prevent overflow
            label_text = f'{width:.1f}%\n({initial:,}→{final:,})'
            
            ax.text(label_x, bar.get_y() + bar.get_height()/2.,
                   label_text,
                   ha=ha_pos, va='center', 
                   fontweight='bold', fontsize=8,
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8, edgecolor='gray'))
        
        # Adjust layout to prevent label cutoff - more space on left for y-axis labels
        plt.subplots_adjust(left=0.20, right=0.85, top=0.92, bottom=0.08)
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "neighborhood_growth_quantity.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.2)
            logger.info(f"Saved plot: {output_path}")
        
        return fig
    
    def plot_neighborhood_growth_value(self, top_n: int = 15, save: bool = True) -> Optional[plt.Figure]:
        """Plot top neighborhoods by value growth."""
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        df = self.load_analysis_data("evolution_analysis", "top_growth_value.csv")
        if df.empty:
            return None
        
        # Filter to show only positive growth (real growth, not decline)
        # Only include neighborhoods with actual growth (positive values)
        df_positive = df[df["crescimento_valor"] > 0].sort_values("crescimento_valor", ascending=False)
        
        if len(df_positive) == 0:
            logger.warning("No positive growth found. Showing neighborhoods with smallest decline.")
            # If no positive growth, show least negative (smallest decline)
            df_negative = df[df["crescimento_valor"] <= 0].sort_values("crescimento_valor", ascending=True)
            df_top = df_negative.head(top_n)
        else:
            # Take top N positive growth values
            df_top = df_positive.head(top_n)
        
        # For visualization, sort ascending so bars go from smallest to largest (top to bottom)
        df_top = df_top.sort_values("crescimento_valor", ascending=True)
        
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Use gradient colors
        colors = sns.color_palette("RdYlGn", len(df_top))
        bars = ax.barh(df_top["bairro"], df_top["crescimento_valor"], color=colors, alpha=0.7, edgecolor='darkblue', linewidth=1.5)
        ax.set_xlabel("Crescimento em Valor Médio (%)", fontsize=12, fontweight='bold')
        ax.set_ylabel("Bairro", fontsize=12, fontweight='bold')
        ax.set_title(f"Top {top_n} Bairros com Maior Crescimento em Valor Médio de IPTU", 
                    fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3)
        ax.axvline(x=0, color='black', linestyle='--', linewidth=1)
        
        # Calculate x-axis limits with padding for labels
        x_min = df_top["crescimento_valor"].min()
        x_max = df_top["crescimento_valor"].max()
        x_range = x_max - x_min
        padding_right = max(abs(x_range * 0.20), 5)  # 20% padding on right
        padding_left = max(abs(x_range * 0.25), 10)   # 25% padding on left (more for negative values)
        
        # Set x-axis limits with padding
        ax.set_xlim(x_min - padding_left, x_max + padding_right)
        
        # Add value labels positioned to avoid collision with y-axis
        for i, bar in enumerate(bars):
            width = bar.get_width()
            initial = df_top.iloc[i]["valor_medio_inicial"]
            final = df_top.iloc[i]["valor_medio_final"]
            
            # Position labels on the bars (not at the edge to avoid collision with y-axis)
            if width > 0:
                # Positive growth: label on the right edge of the bar
                label_x = width + (x_max + padding_right - width) * 0.02
                ha_pos = 'left'
            else:
                # Negative growth: label on the right edge of the bar (for negative bars)
                # Position at a safe distance from y-axis (using bar end + small offset)
                label_x = width + abs(x_min - padding_left - width) * 0.15
                ha_pos = 'left'  # Always left-aligned to avoid collision
            
            # Format values in shorter form (k for thousands, M for millions)
            def format_currency(value):
                if abs(value) >= 1e6:
                    return f'R${value/1e6:.1f}M'
                elif abs(value) >= 1e3:
                    return f'R${value/1e3:.0f}k'
                else:
                    return f'R${value:,.0f}'
            
            # Shorten label text to prevent overflow
            label_text = f'{width:.1f}%\n({format_currency(initial)}→{format_currency(final)})'
            
            ax.text(label_x, bar.get_y() + bar.get_height()/2.,
                   label_text,
                   ha=ha_pos, va='center', 
                   fontweight='bold', fontsize=8,
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8, edgecolor='gray'))
        
        # Adjust layout to prevent label cutoff - more space on left for y-axis labels
        plt.subplots_adjust(left=0.20, right=0.85, top=0.92, bottom=0.08)
        plt.tight_layout()
        
        if save:
            output_path = self.plots_output_path / "neighborhood_growth_value.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.2)
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
            plots["age_distribution"] = self.plot_age_distribution()
            plots["age_value_relationship"] = self.plot_age_value_relationship()
            plots["neighborhood_growth_quantity"] = self.plot_neighborhood_growth_quantity()
            plots["neighborhood_growth_value"] = self.plot_neighborhood_growth_value()
            
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
        import pandas as pd
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
                h2 { color: #34495e; margin-top: 40px; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
                .plot-section { margin: 30px 0; border-bottom: 2px solid #ecf0f1; padding-bottom: 20px; }
                .plot-section:last-child { border-bottom: none; }
                img { max-width: 100%; height: auto; display: block; margin: 20px auto; }
                .plot-title { font-size: 18px; font-weight: bold; color: #34495e; margin-bottom: 10px; }
                .analysis-section { margin: 30px 0; padding: 20px; background-color: #f8f9fa; border-left: 4px solid #3498db; }
                .analysis-title { font-size: 16px; font-weight: bold; color: #2c3e50; margin-bottom: 10px; }
                table { width: 100%; border-collapse: collapse; margin: 20px 0; }
                th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
                th { background-color: #3498db; color: white; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>IPTU Data Analysis - Visualizations Report</h1>
        """
        
        # Volume Analysis Section
        html_content += """
                <h2>1. Análise de Volume</h2>
        """
        
        plot_titles = {
            "volume_by_year": "Total de Imóveis por Ano",
            "volume_by_type": "Distribuição por Tipo de Uso",
            "top_neighborhoods": "Top Bairros por Volume",
            "volume_by_year_type": "Evolução por Ano e Tipo",
            "distribution_by_construction": "Distribuição por Tipo de Construção",
            "temporal_distribution": "Distribuição Temporal"
        }
        
        for plot_name, fig in plots.items():
            if plot_name in plot_titles and fig is not None:
                plot_path = self.plots_output_path / f"{plot_name}.png"
                if plot_path.exists():
                    html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">{plot_titles.get(plot_name, plot_name)}</div>
                        <img src="{plot_path.name}" alt="{plot_name}">
                    </div>
                    """
        
        # Tax Value Analysis Section
        html_content += """
                <h2>2. Análise de Valores de IPTU</h2>
        """
        
        tax_plot_titles = {
            "tax_trends": "Tendências de Valores de IPTU (Boxplot)",
            "top_tax_neighborhoods": "Bairros com Maiores Valores de IPTU"
        }
        
        # Track which plots were added to avoid duplicates
        added_plots = set()
        
        for plot_name, fig in plots.items():
            if plot_name in tax_plot_titles and fig is not None:
                plot_path = self.plots_output_path / f"{plot_name}.png"
                if plot_path.exists():
                    html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">{tax_plot_titles.get(plot_name, plot_name)}</div>
                        <img src="{plot_path.name}" alt="{plot_name}">
                    </div>
                    """
                    added_plots.add(plot_name)
        
        # Include boxplot if file exists but wasn't added in the loop above (fix for missing boxplot)
        plot_path = self.plots_output_path / "tax_trends.png"
        if plot_path.exists() and "tax_trends" not in added_plots:
            html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">Tendências de Valores de IPTU (Boxplot)</div>
                        <img src="{plot_path.name}" alt="tax_trends">
                    </div>
                    """
        
        # Age Analysis Section
        html_content += """
                <h2>3. Análise de Idade de Construção</h2>
        """
        
        # Include visualization if available
        plot_path = self.plots_output_path / "age_distribution.png"
        if plot_path.exists():
            html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">Distribuição por Faixas de Idade de Construção</div>
                        <img src="{plot_path.name}" alt="age_distribution">
                    </div>
                    """
        
        # Load and display age analysis data (table)
        age_dist_file = self.analysis_path / "age_analysis" / "age_distribution_by_range.csv"
        if age_dist_file.exists():
            age_df = pd.read_csv(age_dist_file)
            total = age_df["quantidade"].sum()
            age_df["percentual"] = (age_df["quantidade"] / total * 100).round(2)
            html_content += """
                    <div class="analysis-section">
                        <div class="analysis-title">Distribuição por Faixas de Idade de Construção (Tabela Detalhada)</div>
                        <table>
                            <tr><th>Faixa de Idade</th><th>Quantidade</th><th>Percentual</th></tr>
            """
            for _, row in age_df.head(10).iterrows():
                html_content += f"""
                            <tr>
                                <td>{row.get('faixa_idade', 'N/A')}</td>
                                <td>{row.get('quantidade', 0):,}</td>
                                <td>{row.get('percentual', 0):.2f}%</td>
                            </tr>
                """
            html_content += """
                        </table>
                    </div>
            """
        
        # Age-Value Relationship Section
        html_content += """
                <h2>4. Relação entre Idade e Valor</h2>
        """
        
        # Include visualization if available
        plot_path = self.plots_output_path / "age_value_relationship.png"
        if plot_path.exists():
            html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">Relação entre Idade de Construção e Valor de IPTU</div>
                        <img src="{plot_path.name}" alt="age_value_relationship">
                    </div>
                    """
        
        # Load and display age-value analysis data (table)
        age_value_file = self.analysis_path / "age_value_analysis" / "age_value_relationship.csv"
        if age_value_file.exists():
            age_value_df = pd.read_csv(age_value_file)
            html_content += """
                    <div class="analysis-section">
                        <div class="analysis-title">Valor Médio de IPTU por Faixa de Idade (Tabela Detalhada)</div>
                        <table>
                            <tr><th>Faixa de Idade</th><th>Valor Médio (R$)</th><th>Quantidade</th></tr>
            """
            for _, row in age_value_df.iterrows():
                valor = row.get('valor_medio', 0)
                html_content += f"""
                            <tr>
                                <td>{row.get('faixa_idade', 'N/A')}</td>
                                <td>R$ {valor:,.2f}</td>
                                <td>{row.get('quantidade', 0):,}</td>
                            </tr>
                """
            html_content += """
                        </table>
                    </div>
            """
        
        # Evolution Analysis Section
        html_content += """
                <h2>5. Análise de Evolução de Bairros</h2>
        """
        
        # Include visualization for quantity growth if available
        plot_path_qty = self.plots_output_path / "neighborhood_growth_quantity.png"
        if plot_path_qty.exists():
            html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">Top Bairros com Maior Crescimento em Número de Imóveis</div>
                        <img src="{plot_path_qty.name}" alt="neighborhood_growth_quantity">
                    </div>
                    """
        
        # Include visualization for value growth if available
        plot_path_val = self.plots_output_path / "neighborhood_growth_value.png"
        if plot_path_val.exists():
            html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">Top Bairros com Maior Crescimento em Valor Médio</div>
                        <img src="{plot_path_val.name}" alt="neighborhood_growth_value">
                    </div>
                    """
        
        # Load and display evolution analysis data (tables)
        evolution_file = self.analysis_path / "evolution_analysis" / "top_growth_quantity.csv"
        if evolution_file.exists():
            evolution_df = pd.read_csv(evolution_file)
            html_content += """
                    <div class="analysis-section">
                        <div class="analysis-title">Top 10 Bairros com Maior Crescimento em Número de Imóveis (Tabela Detalhada)</div>
                        <table>
                            <tr><th>Bairro</th><th>Quantidade Inicial</th><th>Quantidade Final</th><th>Crescimento (%)</th></tr>
            """
            for _, row in evolution_df.head(10).iterrows():
                html_content += f"""
                            <tr>
                                <td>{row.get('bairro', 'N/A')}</td>
                                <td>{row.get('quantidade_inicial', 0):,}</td>
                                <td>{row.get('quantidade_final', 0):,}</td>
                                <td>{row.get('crescimento_quantidade', 0):.2f}%</td>
                            </tr>
                """
            html_content += """
                        </table>
                    </div>
            """
        
        evolution_value_file = self.analysis_path / "evolution_analysis" / "top_growth_value.csv"
        if evolution_value_file.exists():
            evolution_value_df = pd.read_csv(evolution_value_file)
            html_content += """
                    <div class="analysis-section">
                        <div class="analysis-title">Top 10 Bairros com Maior Crescimento em Valor Médio (Tabela Detalhada)</div>
                        <table>
                            <tr><th>Bairro</th><th>Valor Médio Inicial (R$)</th><th>Valor Médio Final (R$)</th><th>Crescimento (%)</th></tr>
            """
            for _, row in evolution_value_df.head(10).iterrows():
                html_content += f"""
                            <tr>
                                <td>{row.get('bairro', 'N/A')}</td>
                                <td>R$ {row.get('valor_medio_inicial', 0):,.2f}</td>
                                <td>R$ {row.get('valor_medio_final', 0):,.2f}</td>
                                <td>{row.get('crescimento_valor', 0):.2f}%</td>
                            </tr>
                """
            html_content += """
                        </table>
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

