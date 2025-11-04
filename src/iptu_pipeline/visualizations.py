"""
Visualization module for IPTU analysis results.
Generates interactive plots using Plotly with professional color palettes.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, List
import warnings
warnings.filterwarnings('ignore')

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

# Professional color palette
COLOR_PALETTE = {
    'primary': px.colors.qualitative.Plotly,  # Professional blue-based palette
    'sequential': px.colors.sequential.Viridis,  # For continuous data
    'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#e6f598', '#abdda4', '#66c2a5', '#3288bd'],  # Red-Yellow-Green diverging
    'categorical': px.colors.qualitative.Set3,  # For categories
    'pastel': px.colors.qualitative.Pastel,  # Soft colors
}

# Default plot template
PLOT_TEMPLATE = 'plotly_white'  # Clean white background
PLOT_CONFIG = {
    'displayModeBar': True,
    'displaylogo': False,
    'modeBarButtonsToRemove': ['lasso2d', 'select2d']
}

# Check if kaleido is available for PNG export
try:
    import kaleido
    KALEIDO_AVAILABLE = True
except ImportError:
    KALEIDO_AVAILABLE = False


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
        
        if not PLOTLY_AVAILABLE:
            logger.warning("Plotly not available. Some visualizations will be skipped.")
    
    def _save_plot(self, fig: go.Figure, plot_name: str, save: bool = True) -> Dict[str, Optional[Path]]:
        """
        Save plot as both HTML and PNG (if kaleido available).
        
        Args:
            fig: Plotly figure to save
            plot_name: Name of the plot (without extension)
            save: Whether to save the plot
        
        Returns:
            Dict with 'html' and 'png' keys containing paths or None
        """
        if not save or fig is None:
            return {'html': None, 'png': None}
        
        result = {'html': None, 'png': None}
        
        # Save HTML
        html_path = self.plots_output_path / f"{plot_name}.html"
        try:
            fig.write_html(html_path, config=PLOT_CONFIG)
            result['html'] = html_path
            logger.info(f"Saved plot HTML: {html_path}")
        except Exception as e:
            logger.warning(f"Failed to save HTML for {plot_name}: {e}")
        
        # Save PNG if kaleido is available
        if KALEIDO_AVAILABLE:
            png_path = self.plots_output_path / f"{plot_name}.png"
            try:
                # Set width and height for PNG export (higher quality)
                fig.write_image(
                    str(png_path),
                    width=1200,
                    height=800,
                    scale=2,  # 2x scale for higher DPI
                    engine='kaleido'
                )
                result['png'] = png_path
                logger.info(f"Saved plot PNG: {png_path}")
            except Exception as e:
                logger.warning(f"Failed to save PNG for {plot_name}: {e}")
        else:
            logger.debug("Kaleido not available, skipping PNG export")
        
        return result
    
    def load_analysis_data(self, analysis_type: str, filename: str) -> pd.DataFrame:
        """
        Load analysis data from Parquet or CSV file in gold layer.
        Prefers Parquet format if available, falls back to CSV.
        """
        from iptu_pipeline.config import settings
        
        # Try Parquet first (preferred)
        parquet_path = settings.gold_parquet_dir / "analyses" / analysis_type / f"{filename.replace('.csv', '.parquet')}"
        if parquet_path.exists():
            try:
                return pd.read_parquet(parquet_path)
            except Exception as e:
                logger.warning(f"Failed to load Parquet {parquet_path}: {e}, trying CSV")
        
        # Fallback to CSV
        csv_path = settings.gold_csv_dir / analysis_type / filename
        if csv_path.exists():
            try:
                return pd.read_csv(csv_path)
            except Exception as e:
                logger.error(f"Failed to load CSV {csv_path}: {e}")
                return pd.DataFrame()
        
        # File not found
        if (settings.gold_csv_dir / analysis_type).exists() or (settings.gold_parquet_dir / "analyses" / analysis_type).exists():
            logger.warning(f"Analysis file not found: {filename} (checked Parquet and CSV)")
        return pd.DataFrame()
    
    def load_from_gold(self, path: Path, use_spark: bool = False) -> pd.DataFrame:
        """
        Load data directly from gold layer using pandas or spark.
        
        Args:
            path: Path to gold layer parquet file
            use_spark: Whether to use PySpark for reading (if available)
        
        Returns:
            DataFrame with data from gold layer
        """
        if not path.exists():
            logger.warning(f"Gold layer file not found: {path}")
            return pd.DataFrame()
        
        try:
            if use_spark:
                try:
                    from pyspark.sql import SparkSession
                    spark = SparkSession.builder.getOrCreate()
                    df = spark.read.parquet(str(path))
                    # Convert to pandas for visualization
                    return df.toPandas()
                except Exception as e:
                    logger.warning(f"PySpark read failed, falling back to pandas: {e}")
            
            # Use pandas by default
            return pd.read_parquet(path)
        except Exception as e:
            logger.error(f"Could not load from gold layer {path}: {e}")
            return pd.DataFrame()
    
    def _load_gold_data(self, required_columns: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Load consolidated data from gold layer.
        
        Args:
            required_columns: List of required column names. If any are missing, returns None.
        
        Returns:
            DataFrame with data, or None if loading fails.
        """
        from iptu_pipeline.config import settings
        
        if required_columns is None:
            required_columns = ["valor IPTU"]
        
        # Load from gold layer parquet
        gold_path = settings.gold_parquet_dir / "iptu_consolidated.parquet"
        
        if not gold_path.exists():
            logger.error(f"Gold layer consolidated data not found: {gold_path}")
            return None
        
        try:
            df = pd.read_parquet(gold_path)
            
            # Check if DataFrame is empty
            if df.empty:
                logger.error(f"Gold layer file is EMPTY")
                return None
            
            # Check if columns are corrupted (UUID column names)
            if len(df.columns) > 0 and df.columns[0].startswith('col-') and len(df.columns[0]) > 40:
                logger.error(f"Gold layer has corrupted column names (UUID)")
                return None
            
            # Check for required columns
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                logger.error(f"Gold layer missing required columns: {missing_cols}")
                return None
            
            logger.info(f"Loaded gold data: {len(df):,} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Could not load from gold layer: {e}")
            return None
    
    def plot_volume_by_year(self, save: bool = True) -> Optional[go.Figure]:
        """Plot total properties by year."""
        if not PLOTLY_AVAILABLE:
            return None
        
        df = self.load_analysis_data("volume_analysis", "volume_by_year.csv")
        if df.empty:
            return None
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=df["ano"],
            y=df["total_imoveis"],
            text=[f'{int(x):,}' for x in df["total_imoveis"]],
            textposition='outside',
            marker=dict(
                color=COLOR_PALETTE['primary'][0],
                line=dict(color='#1f77b4', width=1.5)
            ),
            hovertemplate='<b>Ano:</b> %{x}<br><b>Total de Imóveis:</b> %{y:,}<extra></extra>'
        ))
        
        fig.update_layout(
            title={
                'text': "Total de Imóveis por Ano (Histórico)",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16, 'family': 'Arial, sans-serif'}
            },
            xaxis_title="Ano",
            yaxis_title="Total de Imóveis",
            template=PLOT_TEMPLATE,
            height=500,
            showlegend=False,
            hovermode='x unified'
        )
        
        if save:
            self._save_plot(fig, "volume_by_year", save=True)
        
        return fig
    
    def plot_volume_by_type(self, top_n: int = 10, save: bool = True) -> Optional[go.Figure]:
        """Plot distribution of properties by type."""
        if not PLOTLY_AVAILABLE:
            return None
        
        df = self.load_analysis_data("volume_analysis", "volume_by_type.csv")
        if df.empty:
            return None
        
        df_top = df.head(top_n).sort_values("total_imoveis", ascending=False)
        
        # Create subplots with more spacing and constrained domains
        from plotly.subplots import make_subplots
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{"type": "pie"}, {"type": "bar"}]],
            subplot_titles=(
                f"Distribuição por Tipo de Uso (Maiores)",
                f"Volume por Tipo de Uso (Maiores)"
            ),
            horizontal_spacing=0.3,  # Increased spacing between subplots to prevent label overlap
            vertical_spacing=0.2  # Increased spacing for titles to avoid overlap
        )
        
        # Pie chart - keep text inside to prevent overlap with bar chart
        colors = COLOR_PALETTE['categorical'][:len(df_top)]
        fig.add_trace(
            go.Pie(
                labels=df_top["tipo_uso"],
                values=df_top["total_imoveis"],
                marker=dict(colors=colors),
                textinfo='percent',  # Show only percent to reduce clutter
                textposition='inside',  # Keep text inside to prevent overlap with adjacent chart
                textfont=dict(size=11),
                insidetextorientation='auto',  # Auto-orient text inside slices
                hovertemplate='<b>%{label}</b><br>Total: %{value:,}<br>Percentual: %{percent}<extra></extra>',
                pull=[0.05 if i == 0 else 0 for i in range(len(df_top))]  # Slight pull on largest slice
            ),
            row=1, col=1
        )
        
        # Bar chart (sorted ascending for better visualization)
        df_top_sorted = df_top.sort_values("total_imoveis", ascending=True)
        fig.add_trace(
            go.Bar(
                y=df_top_sorted["tipo_uso"],
                x=df_top_sorted["total_imoveis"],
                orientation='h',
                text=[f'{int(x):,}' for x in df_top_sorted["total_imoveis"]],
                textposition='outside',
                marker=dict(
                    color=COLOR_PALETTE['primary'][2],
                    line=dict(color=COLOR_PALETTE['primary'][2], width=1)
                ),
                hovertemplate='<b>%{y}</b><br>Total de Imóveis: %{x:,}<extra></extra>'
            ),
            row=1, col=2
        )
        
        fig.update_layout(
            template=PLOT_TEMPLATE,
            height=700,  # Increased height to accommodate labels and titles
            showlegend=False,
            title_text=f"Distribuição de Imóveis por Tipo de Uso (Maiores)",
            title_x=0.5,
            title_font_size=16,
            margin=dict(l=50, r=50, t=120, b=50)  # Increased top margin to prevent title overlap
        )
        
        # Update subplot title positions to prevent overlap
        fig.update_annotations(font_size=13, yshift=10)  # Move titles up and make slightly smaller
        
        fig.update_xaxes(title_text="Total de Imóveis", row=1, col=2)
        
        if save:
            self._save_plot(fig, "volume_by_type", save=True)
        
        return fig
    
    def plot_top_neighborhoods(self, top_n: int = 20, save: bool = True) -> Optional[go.Figure]:
        """Plot top neighborhoods by volume."""
        if not PLOTLY_AVAILABLE:
            return None
        
        df = self.load_analysis_data("volume_analysis", "volume_by_neighborhood.csv")
        if df.empty:
            return None
        
        df_top = df.head(top_n).sort_values("total_imoveis", ascending=True)
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            y=df_top["bairro"],
            x=df_top["total_imoveis"],
            orientation='h',
            text=[f'{int(row["total_imoveis"]):,} ({row["percentual"]:.1f}%)' 
                  for _, row in df_top.iterrows()],
            textposition='outside',
            marker=dict(
                color=COLOR_PALETTE['sequential'][3],
                line=dict(color=COLOR_PALETTE['sequential'][5], width=1.5),
                colorscale='Viridis'
            ),
            hovertemplate='<b>%{y}</b><br>Total: %{x:,}<br>Percentual: %{customdata:.2f}%<extra></extra>',
            customdata=df_top["percentual"]
        ))
        
        fig.update_layout(
            title={
                'text': f"Maiores Bairros por Volume de Imóveis",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16}
            },
            xaxis_title="Total de Imóveis",
            yaxis_title="Bairro",
            template=PLOT_TEMPLATE,
            height=max(400, top_n * 25),
            showlegend=False,
            hovermode='y unified'
        )
        
        if save:
            self._save_plot(fig, "top_neighborhoods", save=True)
        
        return fig
    
    def plot_volume_by_year_and_type(self, save: bool = True) -> Optional[go.Figure]:
        """Plot volume by year and type (stacked area chart)."""
        if not PLOTLY_AVAILABLE:
            return None
        
        df = self.load_analysis_data("volume_analysis", "volume_by_year_type.csv")
        if df.empty:
            return None
        
        # Pivot for stacked chart
        pivot_df = df.pivot(index="ano", columns="tipo_uso", values="total_imoveis").fillna(0)
        # Get top 5 types
        top_types = df.groupby("tipo_uso")["total_imoveis"].sum().nlargest(5).index
        pivot_df = pivot_df[top_types]
        
        fig = go.Figure()
        
        colors = COLOR_PALETTE['categorical'][:len(pivot_df.columns)]
        for i, col in enumerate(pivot_df.columns):
            fig.add_trace(go.Scatter(
                x=pivot_df.index,
                y=pivot_df[col],
                mode='lines',
                name=col,
                stackgroup='one',
                fillcolor=colors[i % len(colors)],
                line=dict(width=0.5, color=colors[i % len(colors)]),
                hovertemplate=f'<b>{col}</b><br>Ano: %{{x}}<br>Total: %{{y:,}}<extra></extra>'
            ))
        
        fig.update_layout(
            title={
                'text': "Evolução do Volume de Imóveis por Tipo de Uso (Top 5)",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16}
            },
            xaxis_title="Ano",
            yaxis_title="Total de Imóveis",
            template=PLOT_TEMPLATE,
            height=600,
            hovermode='x unified',
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02
            )
        )
        
        if save:
            self._save_plot(fig, "volume_by_year_type", save=True)
        
        return fig
    
    def plot_tax_trends(self, save: bool = True) -> Optional[go.Figure]:
        """Plot IPTU tax value trends as boxplot by year."""
        if not PLOTLY_AVAILABLE:
            return None
        
        import numpy as np
        
        # Load raw data from gold layer
        df = self._load_gold_data(required_columns=["valor IPTU", "ano do exercício"])
        
        if df is None:
            logger.error("Could not load data for tax trends plot. Please ensure gold layer is available.")
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
        
        # Filter extreme outliers using IQR method before creating boxplot
        filtered_data_list = []
        for year_data in box_data:
            if len(year_data) == 0:
                filtered_data_list.append([])
                continue
            
            # Calculate IQR
            q1 = np.percentile(year_data, 25)
            q3 = np.percentile(year_data, 75)
            iqr = q3 - q1
            
            # Filter outliers (values beyond 3*IQR from Q1/Q3)
            lower_bound = q1 - 3 * iqr
            upper_bound = q3 + 3 * iqr
            
            filtered_data = year_data[(year_data >= lower_bound) & (year_data <= upper_bound)]
            filtered_data_list.append(filtered_data)
        
        # Create Plotly boxplot
        fig = go.Figure()
        
        colors = COLOR_PALETTE['sequential'][::len(COLOR_PALETTE['sequential'])//len(years)][:len(years)]
        
        for i, (year, year_data) in enumerate(zip(years, filtered_data_list)):
            if len(year_data) > 0:
                fig.add_trace(go.Box(
                    y=year_data,
                    name=str(int(year)),
                    boxpoints='outliers',
                    marker_color=colors[i % len(colors)],
                    line=dict(color=COLOR_PALETTE['primary'][1], width=2),
                    fillcolor=colors[i % len(colors)],
                    opacity=0.7,
                    hovertemplate=f'<b>Ano:</b> {int(year)}<br>Valor: R$ %{{y:,.0f}}<extra></extra>'
                ))
        
        # Use log scale if data has large range
        max_val = df["valor_iptu_numeric"].max()
        median_val = df["valor_iptu_numeric"].median()
        use_log = max_val > median_val * 50
        
        fig.update_layout(
            title={
                'text': "Distribuição de Valores de IPTU por Ano (Boxplot)<br><sub>Apenas valores não-zero</sub>",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16}
            },
            xaxis_title="Ano",
            yaxis_title="Valor IPTU (R$)",
            template=PLOT_TEMPLATE,
            height=600,
            showlegend=False,
            yaxis_type='log' if use_log else 'linear',
            hovermode='x unified'
        )
        
        if use_log:
            fig.update_yaxes(
                tickformat='.0f',
                ticktext=[f'R${x/1e3:.0f}k' if x < 1e6 else f'R${x/1e6:.1f}M' 
                         for x in fig.data[0].y if len(fig.data[0].y) > 0]
            )
        
        if save:
            self._save_plot(fig, "tax_trends", save=True)
        
        return fig
    
    def plot_top_tax_neighborhoods(self, top_n: int = 20, save: bool = True) -> Optional[go.Figure]:
        """Plot top neighborhoods by average tax value."""
        if not PLOTLY_AVAILABLE:
            return None
        
        df = self.load_analysis_data("tax_value_analysis", "avg_tax_by_neighborhood_top20.csv")
        if df.empty:
            return None
        
        df_top = df.head(top_n).sort_values("media", ascending=True)
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            y=df_top["bairro"],
            x=df_top["media"],
            orientation='h',
            text=[f'R$ {row["media"]:,.0f} (n={int(row["count"]):,})' 
                  for _, row in df_top.iterrows()],
            textposition='outside',
            marker=dict(
                color=COLOR_PALETTE['diverging'][5],
                line=dict(color=COLOR_PALETTE['diverging'][7], width=1.5),
                colorscale='Reds'
            ),
            hovertemplate='<b>%{y}</b><br>Valor Médio: R$ %{x:,.0f}<br>Quantidade: %{customdata:,}<extra></extra>',
            customdata=df_top["count"]
        ))
        
        fig.update_layout(
            title={
                'text': f"Maiores Bairros por Valor Médio de IPTU",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16}
            },
            xaxis_title="Valor Médio de IPTU (R$)",
            yaxis_title="Bairro",
            template=PLOT_TEMPLATE,
            height=max(400, top_n * 25),
            showlegend=False,
            hovermode='y unified'
        )
        
        fig.update_xaxes(tickformat='$,.0f', tickprefix='R$ ')
        
        if save:
            self._save_plot(fig, "top_tax_neighborhoods", save=True)
        
        return fig
    
    def plot_distribution_by_construction(self, save: bool = True) -> Optional[go.Figure]:
        """Plot distribution by construction type."""
        if not PLOTLY_AVAILABLE:
            return None
        
        df = self.load_analysis_data("distribution_analysis", "distribution_by_construction.csv")
        if df.empty:
            return None
        
        df = df.sort_values("quantidade", ascending=False)
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=df["tipo_construcao"],
            y=df["quantidade"],
            text=[f'{int(row["quantidade"]):,}<br>({row["percentual"]:.1f}%)' 
                  for _, row in df.iterrows()],
            textposition='outside',
            marker=dict(
                color=COLOR_PALETTE['primary'][4],
                line=dict(color=COLOR_PALETTE['primary'][1], width=1.5)
            ),
            hovertemplate='<b>%{x}</b><br>Quantidade: %{y:,}<br>Percentual: %{customdata:.2f}%<extra></extra>',
            customdata=df["percentual"]
        ))
        
        fig.update_layout(
            title={
                'text': "Distribuição de Imóveis por Tipo de Construção",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16}
            },
            xaxis_title="Tipo de Construção",
            yaxis_title="Quantidade",
            template=PLOT_TEMPLATE,
            height=600,
            showlegend=False,
            hovermode='x unified',
            xaxis_tickangle=-45
        )
        
        if save:
            self._save_plot(fig, "distribution_by_construction", save=True)
        
        return fig
    
    def plot_temporal_distribution(self, save: bool = True) -> Optional[go.Figure]:
        """Plot temporal distribution of properties."""
        if not PLOTLY_AVAILABLE:
            return None
        
        df = self.load_analysis_data("distribution_analysis", "distribution_by_year.csv")
        if df.empty:
            return None
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df["ano"],
            y=df["quantidade"],
            mode='lines+markers+text',
            name='Quantidade',
            text=[f'{int(row["quantidade"]):,}<br>({row["percentual"]:.1f}%)' 
                  for _, row in df.iterrows()],
            textposition='top center',
            line=dict(
                color=COLOR_PALETTE['primary'][0],
                width=3
            ),
            marker=dict(
                size=12,
                color=COLOR_PALETTE['primary'][1],
                line=dict(width=2, color=COLOR_PALETTE['primary'][0])
            ),
            fill='tozeroy',
            fillcolor='rgba(31, 119, 180, 0.3)',
            hovertemplate='<b>Ano:</b> %{x}<br>Quantidade: %{y:,}<br>Percentual: %{customdata:.2f}%<extra></extra>',
            customdata=df["percentual"]
        ))
        
        fig.update_layout(
            title={
                'text': "Distribuição Temporal de Imóveis",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16}
            },
            xaxis_title="Ano",
            yaxis_title="Quantidade de Imóveis",
            template=PLOT_TEMPLATE,
            height=500,
            showlegend=False,
            hovermode='x unified'
        )
        
        if save:
            self._save_plot(fig, "temporal_distribution", save=True)
        
        return fig
    
    def plot_age_distribution(self, save: bool = True) -> Optional[go.Figure]:
        """Plot distribution of properties by construction age ranges."""
        if not PLOTLY_AVAILABLE:
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
        
        # Create subplots
        from plotly.subplots import make_subplots
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{"type": "bar"}, {"type": "pie"}]],
            subplot_titles=(
                "Distribuição por Faixas de Idade de Construção",
                "Distribuição Percentual por Idade"
            )
        )
        
        # Bar chart
        colors = COLOR_PALETTE['sequential'][::len(COLOR_PALETTE['sequential'])//len(df)][:len(df)]
        fig.add_trace(
            go.Bar(
                x=df["faixa_idade"],
                y=df["quantidade"],
                text=[f'{int(x):,}' for x in df["quantidade"]],
                textposition='outside',
                marker=dict(
                    color=colors,
                    line=dict(color=COLOR_PALETTE['primary'][1], width=1.5)
                ),
                hovertemplate='<b>%{x}</b><br>Quantidade: %{y:,}<br>Percentual: %{customdata:.2f}%<extra></extra>',
                customdata=df["percentual"]
            ),
            row=1, col=1
        )
        
        # Pie chart
        fig.add_trace(
            go.Pie(
                labels=df["faixa_idade"],
                values=df["quantidade"],
                marker=dict(colors=colors),
                textinfo='label+percent',
                hovertemplate='<b>%{label}</b><br>Quantidade: %{value:,}<br>Percentual: %{percent}<extra></extra>'
            ),
            row=1, col=2
        )
        
        fig.update_layout(
            template=PLOT_TEMPLATE,
            height=600,
            showlegend=False,
            title_text="Distribuição de Imóveis por Faixas de Idade de Construção",
            title_x=0.5
        )
        
        fig.update_xaxes(title_text="Faixa de Idade", row=1, col=1, tickangle=-45)
        fig.update_yaxes(title_text="Quantidade de Imóveis", row=1, col=1)
        
        if save:
            self._save_plot(fig, "age_distribution", save=True)
        
        return fig
    
    def plot_age_value_relationship(self, save: bool = True) -> Optional[go.Figure]:
        """
        Plot relationship between construction age, quantity and IPTU value.
        Uses data from gold layer. Creates a grouped bar chart showing both metrics.
        """
        if not PLOTLY_AVAILABLE:
            return None
        
        # Try to load from gold layer CSV first (analysis output)
        df = self.load_analysis_data("age_value_analysis", "age_value_relationship.csv")
        
        # If not found in CSV, try to load from gold layer parquet if available
        if df.empty:
            from iptu_pipeline.config import GOLD_DIR
            gold_path = GOLD_DIR / "analyses" / "age_value_analysis" / "age_value_relationship.parquet"
            if gold_path.exists():
                df = self.load_from_gold(gold_path)
            else:
                logger.warning(f"Age-value relationship data not found in gold layer. Run pipeline to generate it.")
                return None
        
        if df.empty:
            return None
        
        # Sort by age range order
        age_order = ["0-10", "11-20", "21-30", "31-40", "41-50", "50+"]
        df["order"] = df["faixa_idade"].apply(lambda x: age_order.index(x) if x in age_order else 999)
        df = df.sort_values("order")
        
        # Create a grouped bar chart with secondary y-axis
        from plotly.subplots import make_subplots
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add valor medio bars
        fig.add_trace(
            go.Bar(
                x=df["faixa_idade"],
                y=df["valor_medio"],
                name='Valor Médio de IPTU',
                text=[f'R$ {x:,.0f}' for x in df["valor_medio"]],
                textposition='outside',
                marker=dict(
                    color=COLOR_PALETTE['primary'][0],
                    line=dict(color=COLOR_PALETTE['primary'][1], width=1.5)
                ),
                hovertemplate='<b>%{x}</b><br>Valor Médio: R$ %{y:,.0f}<extra></extra>'
            ),
            secondary_y=False
        )
        
        # Add quantidade bars on secondary y-axis
        fig.add_trace(
            go.Bar(
                x=df["faixa_idade"],
                y=df["quantidade"],
                name='Quantidade de Imóveis',
                text=[f'{int(x):,}' for x in df["quantidade"]],
                textposition='outside',
                marker=dict(
                    color=COLOR_PALETTE['primary'][2],
                    line=dict(color=COLOR_PALETTE['primary'][3], width=1.5),
                    opacity=0.7
                ),
                hovertemplate='<b>%{x}</b><br>Quantidade: %{y:,}<extra></extra>'
            ),
            secondary_y=True
        )
        
        fig.update_layout(
            title={
                'text': "Relação: Quantidade vs Valor Médio por Faixa de Idade",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16}
            },
            xaxis_title="Faixa de Idade de Construção (anos)",
            template=PLOT_TEMPLATE,
            height=600,
            barmode='group',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            hovermode='x unified'
        )
        
        fig.update_yaxes(title_text="Valor Médio de IPTU (R$)", secondary_y=False, tickformat='$,.0f', tickprefix='R$ ')
        fig.update_yaxes(title_text="Quantidade de Imóveis", secondary_y=True, tickformat=',.0f')
        
        if save:
            self._save_plot(fig, "age_value_relationship", save=True)
        
        return fig
    
    def plot_neighborhood_growth_quantity(self, top_n: int = 15, save: bool = True) -> Optional[go.Figure]:
        """Plot top neighborhoods by quantity growth."""
        if not PLOTLY_AVAILABLE:
            return None
        
        df = self.load_analysis_data("evolution_analysis", "top_growth_quantity.csv")
        if df.empty:
            return None
        
        # Filter to show only positive growth
        df_positive = df[df["crescimento_quantidade"] > 0].sort_values("crescimento_quantidade", ascending=False)
        
        if len(df_positive) == 0:
            logger.warning("No positive growth found. Showing neighborhoods with smallest decline.")
            df_negative = df[df["crescimento_quantidade"] <= 0].sort_values("crescimento_quantidade", ascending=True)
            df_top = df_negative.head(top_n)
        else:
            df_top = df_positive.head(top_n)
        
        df_top = df_top.sort_values("crescimento_quantidade", ascending=True)
        
        # Colors for positive/negative growth
        colors = [COLOR_PALETTE['diverging'][8] if x < 0 else COLOR_PALETTE['diverging'][2] 
                 for x in df_top["crescimento_quantidade"]]
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            y=df_top["bairro"],
            x=df_top["crescimento_quantidade"],
            orientation='h',
            text=[f'{row["crescimento_quantidade"]:.1f}%<br>({int(row["quantidade_inicial"]):,}→{int(row["quantidade_final"]):,})' 
                  for _, row in df_top.iterrows()],
            textposition='outside',
            marker=dict(
                color=colors,
                line=dict(color=COLOR_PALETTE['primary'][1], width=1.5)
            ),
            hovertemplate='<b>%{y}</b><br>Crescimento: %{x:.2f}%<br>Inicial: %{customdata[0]:,}<br>Final: %{customdata[1]:,}<extra></extra>',
            customdata=df_top[["quantidade_inicial", "quantidade_final"]].values
        ))
        
        fig.update_layout(
            title={
                'text': f"Maiores Bairros com Maior Crescimento em Número de Imóveis",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16}
            },
            xaxis_title="Crescimento em Quantidade (%)",
            yaxis_title="Bairro",
            template=PLOT_TEMPLATE,
            height=max(500, top_n * 30),
            showlegend=False,
            hovermode='y unified',
            shapes=[dict(type='line', x0=0, x1=0, y0=-0.5, y1=len(df_top)-0.5, 
                        line=dict(color='black', width=2, dash='dash'))]
        )
        
        if save:
            self._save_plot(fig, "neighborhood_growth_quantity", save=True)
        
        return fig
    
    def plot_neighborhood_growth_value(self, top_n: int = 15, save: bool = True) -> Optional[go.Figure]:
        """Plot top neighborhoods by value growth."""
        if not PLOTLY_AVAILABLE:
            return None
        
        df = self.load_analysis_data("evolution_analysis", "top_growth_value.csv")
        if df.empty:
            return None
        
        # Filter to show only positive growth
        df_positive = df[df["crescimento_valor"] > 0].sort_values("crescimento_valor", ascending=False)
        
        if len(df_positive) == 0:
            logger.warning("No positive growth found. Showing neighborhoods with smallest decline.")
            df_negative = df[df["crescimento_valor"] <= 0].sort_values("crescimento_valor", ascending=True)
            df_top = df_negative.head(top_n)
        else:
            df_top = df_positive.head(top_n)
        
        df_top = df_top.sort_values("crescimento_valor", ascending=True)
        
        # Format currency helper
        def format_currency(value):
            if abs(value) >= 1e6:
                return f'R${value/1e6:.1f}M'
            elif abs(value) >= 1e3:
                return f'R${value/1e3:.0f}k'
            else:
                return f'R${value:,.0f}'
        
        # Colors for positive/negative growth
        colors = [COLOR_PALETTE['diverging'][8] if x < 0 else COLOR_PALETTE['diverging'][2] 
                 for x in df_top["crescimento_valor"]]
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            y=df_top["bairro"],
            x=df_top["crescimento_valor"],
            orientation='h',
            text=[f'{row["crescimento_valor"]:.1f}%<br>({format_currency(row["valor_medio_inicial"])}→{format_currency(row["valor_medio_final"])})' 
                  for _, row in df_top.iterrows()],
            textposition='outside',
            marker=dict(
                color=colors,
                line=dict(color=COLOR_PALETTE['primary'][1], width=1.5)
            ),
            hovertemplate='<b>%{y}</b><br>Crescimento: %{x:.2f}%<br>Inicial: R$ %{customdata[0]:,.0f}<br>Final: R$ %{customdata[1]:,.0f}<extra></extra>',
            customdata=df_top[["valor_medio_inicial", "valor_medio_final"]].values
        ))
        
        fig.update_layout(
            title={
                'text': f"Maiores Bairros com Maior Crescimento em Valor Médio de IPTU",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16}
            },
            xaxis_title="Crescimento em Valor Médio (%)",
            yaxis_title="Bairro",
            template=PLOT_TEMPLATE,
            height=max(500, top_n * 30),
            showlegend=False,
            hovermode='y unified',
            shapes=[dict(type='line', x0=0, x1=0, y0=-0.5, y1=len(df_top)-0.5, 
                        line=dict(color='black', width=2, dash='dash'))]
        )
        
        if save:
            self._save_plot(fig, "neighborhood_growth_value", save=True)
        
        return fig
    
    def generate_all_plots(self, close_figs: bool = True) -> Dict[str, Optional[go.Figure]]:
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
            
            # Note: Plotly figures don't need to be closed like matplotlib
            # They are automatically managed by Plotly
            
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
                plot_path = self.plots_output_path / f"{plot_name}.html"
                if plot_path.exists():
                    # Embed Plotly HTML directly
                    with open(plot_path, 'r', encoding='utf-8') as f:
                        plot_html = f.read()
                    html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">{plot_titles.get(plot_name, plot_name)}</div>
                        {plot_html}
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
                plot_path = self.plots_output_path / f"{plot_name}.html"
                if plot_path.exists():
                    with open(plot_path, 'r', encoding='utf-8') as f:
                        plot_html_tax = f.read()
                    html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">{tax_plot_titles.get(plot_name, plot_name)}</div>
                        {plot_html_tax}
                    </div>
                    """
                    added_plots.add(plot_name)
        
        # Include boxplot if file exists but wasn't added in the loop above (fix for missing boxplot)
        plot_path = self.plots_output_path / "tax_trends.html"
        if plot_path.exists() and "tax_trends" not in added_plots:
            with open(plot_path, 'r', encoding='utf-8') as f:
                plot_html = f.read()
            html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">Tendências de Valores de IPTU (Boxplot)</div>
                        {plot_html}
                    </div>
                    """
        
        # Age Analysis Section
        html_content += """
                <h2>3. Análise de Idade de Construção</h2>
        """
        
        # Include visualization if available
        plot_path = self.plots_output_path / "age_distribution.html"
        if plot_path.exists():
            with open(plot_path, 'r', encoding='utf-8') as f:
                plot_html = f.read()
            html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">Distribuição por Faixas de Idade de Construção</div>
                        {plot_html}
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
        plot_path = self.plots_output_path / "age_value_relationship.html"
        if plot_path.exists():
            with open(plot_path, 'r', encoding='utf-8') as f:
                plot_html = f.read()
            html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">Relação entre Idade de Construção e Valor de IPTU</div>
                        {plot_html}
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
        plot_path_qty = self.plots_output_path / "neighborhood_growth_quantity.html"
        if plot_path_qty.exists():
            with open(plot_path_qty, 'r', encoding='utf-8') as f:
                plot_html_qty = f.read()
            html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">Top Bairros com Maior Crescimento em Número de Imóveis</div>
                        {plot_html_qty}
                    </div>
                    """
        
        # Include visualization for value growth if available
        plot_path_val = self.plots_output_path / "neighborhood_growth_value.html"
        if plot_path_val.exists():
            with open(plot_path_val, 'r', encoding='utf-8') as f:
                plot_html_val = f.read()
            html_content += f"""
                    <div class="plot-section">
                        <div class="plot-title">Top Bairros com Maior Crescimento em Valor Médio</div>
                        {plot_html_val}
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
        
        # Note: Plotly figures don't need to be closed like matplotlib
        
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
    for plot_file in viz.plots_output_path.glob("*.html"):
        plot_files[plot_file.stem] = plot_file
    
    plot_files["html_report"] = html_report
    
    return plot_files

