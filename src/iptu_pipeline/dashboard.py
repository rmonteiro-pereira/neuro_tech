"""
Dashboard generator for IPTU data analysis results.
Creates interactive visualizations using Plotly.
"""
import pandas as pd
from pathlib import Path
from typing import Optional
import json

try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

from iptu_pipeline.config import ANALYSIS_OUTPUT_PATH, OUTPUT_DIR
from iptu_pipeline.utils.logger import setup_logger
from iptu_pipeline.pipelines.analysis import IPTUAnalyzer

logger = setup_logger("dashboard")


class IPTUDashboard:
    """Generates interactive dashboard with IPTU analysis visualizations."""
    
    def __init__(self, analyzer: Optional[IPTUAnalyzer] = None, df: Optional[pd.DataFrame] = None):
        """
        Initialize dashboard generator.
        
        Args:
            analyzer: IPTUAnalyzer instance with completed analyses
            df: Consolidated DataFrame (if analyzer not provided)
        """
        if not PLOTLY_AVAILABLE:
            logger.warning("Plotly not available. Dashboard will not be generated.")
        
        if analyzer is None and df is not None:
            self.analyzer = IPTUAnalyzer(df)
            self.analyzer.generate_all_analyses()
        else:
            self.analyzer = analyzer
        
        if self.analyzer is None:
            raise ValueError("Either analyzer or df must be provided")
    
    def create_volume_visualizations(self) -> list:
        """Create visualizations for volume analysis."""
        if not PLOTLY_AVAILABLE:
            return []
        
        figs = []
        
        volume_results = self.analyzer.analyses_results.get("volume_analysis", {})
        
        # 1. Total properties by year
        if "volume_by_year" in volume_results:
            df_year = volume_results["volume_by_year"]
            fig = px.bar(
                df_year,
                x="ano",
                y="total_imoveis",
                title="Total de Imóveis por Ano (Histórico)",
                labels={"ano": "Ano", "total_imoveis": "Total de Imóveis"},
                text="total_imoveis"
            )
            fig.update_traces(texttemplate='%{text:,}', textposition='outside')
            fig.update_layout(
                xaxis_title="Ano",
                yaxis_title="Total de Imóveis",
                showlegend=False
            )
            figs.append(("volume_by_year", fig))
        
        # 2. Volume by type (pie chart)
        if "volume_by_type" in volume_results:
            df_type = volume_results["volume_by_type"]
            fig = px.pie(
                df_type.head(10),
                values="total_imoveis",
                names="tipo_uso",
                title="Distribuição de Imóveis por Tipo de Uso (Top 10)",
                hole=0.4
            )
            figs.append(("volume_by_type", fig))
        
        # 3. Top neighborhoods by volume
        if "volume_by_neighborhood" in volume_results:
            df_neigh = volume_results["volume_by_neighborhood"].head(20)
            fig = px.bar(
                df_neigh,
                x="total_imoveis",
                y="bairro",
                orientation='h',
                title="Top 20 Bairros por Volume de Imóveis",
                labels={"total_imoveis": "Total de Imóveis", "bairro": "Bairro"}
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            figs.append(("top_neighborhoods", fig))
        
        # 4. Volume by year and type (stacked bar)
        if "volume_by_year_type" in volume_results:
            df_year_type = volume_results["volume_by_year_type"]
            # Pivot for stacked bar
            pivot_df = df_year_type.pivot(
                index="ano",
                columns="tipo_uso",
                values="total_imoveis"
            ).fillna(0)
            
            fig = go.Figure()
            for col in pivot_df.columns[:10]:  # Top 10 types
                fig.add_trace(go.Bar(
                    name=col,
                    x=pivot_df.index,
                    y=pivot_df[col]
                ))
            
            fig.update_layout(
                barmode='stack',
                title="Volume de Imóveis por Ano e Tipo de Uso (Top 10 Tipos)",
                xaxis_title="Ano",
                yaxis_title="Total de Imóveis"
            )
            figs.append(("volume_year_type", fig))
        
        return figs
    
    def create_distribution_visualizations(self) -> list:
        """Create visualizations for distribution analysis."""
        if not PLOTLY_AVAILABLE:
            return []
        
        figs = []
        
        dist_results = self.analyzer.analyses_results.get("distribution_analysis", {})
        
        # 1. Temporal distribution (line chart)
        if "distribution_by_year" in dist_results:
            df_year = dist_results["distribution_by_year"]
            fig = px.line(
                df_year,
                x="ano",
                y="quantidade",
                title="Distribuição Temporal de Imóveis",
                markers=True,
                labels={"ano": "Ano", "quantidade": "Quantidade de Imóveis"}
            )
            fig.update_traces(line=dict(width=3))
            figs.append(("temporal_distribution", fig))
        
        # 2. Top neighborhoods heatmap by year
        if "top_neighborhoods_by_year" in dist_results:
            df_neigh_year = dist_results["top_neighborhoods_by_year"]
            # Pivot for heatmap
            pivot_df = df_neigh_year.pivot(
                index="bairro",
                columns="ano",
                values="quantidade"
            ).fillna(0)
            
            fig = px.imshow(
                pivot_df,
                title="Top 10 Bairros por Ano - Heatmap",
                labels=dict(x="Ano", y="Bairro", color="Quantidade"),
                aspect="auto",
                color_continuous_scale="Viridis"
            )
            figs.append(("neighborhood_heatmap", fig))
        
        return figs
    
    def create_tax_trends_visualizations(self) -> list:
        """Create visualizations for tax value trends."""
        if not PLOTLY_AVAILABLE:
            return []
        
        figs = []
        
        tax_results = self.analyzer.analyses_results.get("tax_value_analysis", {})
        
        # 1. Average tax by year
        if "tax_stats_by_year" in tax_results:
            df_tax = tax_results["tax_stats_by_year"]
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df_tax["ano do exercício"],
                y=df_tax["media"],
                mode='lines+markers',
                name='Média',
                line=dict(width=3)
            ))
            
            fig.add_trace(go.Scatter(
                x=df_tax["ano do exercício"],
                y=df_tax["mediana"],
                mode='lines+markers',
                name='Mediana',
                line=dict(width=3, dash='dash')
            ))
            
            fig.update_layout(
                title="Tendência de Valores de IPTU por Ano",
                xaxis_title="Ano",
                yaxis_title="Valor (R$)",
                hovermode='x unified'
            )
            figs.append(("tax_trends", fig))
        
        # 2. Top neighborhoods by average tax
        if "avg_tax_by_neighborhood_top20" in tax_results:
            df_tax_neigh = tax_results["avg_tax_by_neighborhood_top20"]
            fig = px.bar(
                df_tax_neigh,
                x="media",
                y="bairro",
                orientation='h',
                title="Top 20 Bairros por Valor Médio de IPTU",
                labels={"media": "Valor Médio IPTU (R$)", "bairro": "Bairro"}
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            figs.append(("top_tax_neighborhoods", fig))
        
        return figs
    
    def generate_dashboard_html(
        self,
        output_path: Optional[Path] = None,
        include_all: bool = True
    ) -> Path:
        """
        Generate complete dashboard as HTML file.
        
        Args:
            output_path: Path to save HTML dashboard
            include_all: Whether to include all visualizations
        
        Returns:
            Path to generated HTML file
        """
        if not PLOTLY_AVAILABLE:
            logger.error("Plotly not available. Cannot generate dashboard.")
            return None
        
        output_path = output_path or OUTPUT_DIR / "dashboard.html"
        
        logger.info(f"Generating dashboard HTML: {output_path}")
        
        # Create subplots for dashboard layout
        if include_all:
            # Volume visualizations
            volume_figs = self.create_volume_visualizations()
            dist_figs = self.create_distribution_visualizations()
            tax_figs = self.create_tax_trends_visualizations()
            
            # Simple approach: combine into one HTML page
            html_content = "<html><head><title>IPTU Dashboard</title></head><body>"
            html_content += "<h1>IPTU Data Analysis Dashboard</h1>"
            
            for fig_list in [volume_figs, dist_figs, tax_figs]:
                for fig_name, fig_obj in fig_list:
                    html_content += f"<h2>{fig_name.replace('_', ' ').title()}</h2>"
                    html_content += fig_obj.to_html(include_plotlyjs='cdn', div_id=fig_name)
            
            html_content += "</body></html>"
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
        
        logger.info(f"✓ Dashboard saved to {output_path}")
        return output_path
    
    def generate_summary_report(self, output_path: Optional[Path] = None) -> Path:
        """
        Generate a summary text report with key metrics.
        
        Args:
            output_path: Path to save report
        
        Returns:
            Path to generated report
        """
        output_path = output_path or OUTPUT_DIR / "summary_report.txt"
        
        logger.info(f"Generating summary report: {output_path}")
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("IPTU DATA ANALYSIS - SUMMARY REPORT")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        # Volume analysis summary
        volume_results = self.analyzer.analyses_results.get("volume_analysis", {})
        if "total_properties" in volume_results:
            total = volume_results["total_properties"]["value"].iloc[0]
            report_lines.append(f"TOTAL DE IMÓVEIS: {total:,}")
            report_lines.append("")
        
        if "volume_by_year" in volume_results:
            report_lines.append("VOLUME POR ANO:")
            for _, row in volume_results["volume_by_year"].iterrows():
                report_lines.append(f"  {int(row['ano'])}: {int(row['total_imoveis']):,} imóveis")
            report_lines.append("")
        
        if "volume_by_type" in volume_results:
            report_lines.append("TOP 10 TIPOS DE USO:")
            for _, row in volume_results["volume_by_type"].head(10).iterrows():
                report_lines.append(
                    f"  {row['tipo_uso']}: {int(row['total_imoveis']):,} "
                    f"({row['percentual']:.2f}%)"
                )
            report_lines.append("")
        
        if "volume_by_neighborhood" in volume_results:
            report_lines.append("TOP 10 BAIRROS:")
            for _, row in volume_results["volume_by_neighborhood"].head(10).iterrows():
                report_lines.append(
                    f"  {row['bairro']}: {int(row['total_imoveis']):,} "
                    f"({row['percentual']:.2f}%)"
                )
            report_lines.append("")
        
        # Tax trends summary
        tax_results = self.analyzer.analyses_results.get("tax_value_analysis", {})
        if "tax_stats_by_year" in tax_results:
            report_lines.append("TENDÊNCIAS DE VALOR DE IPTU:")
            for _, row in tax_results["tax_stats_by_year"].iterrows():
                report_lines.append(
                    f"  {int(row['ano do exercício'])}: "
                    f"Média R$ {row['media']:,.2f}, "
                    f"Mediana R$ {row['mediana']:,.2f}"
                )
            report_lines.append("")
        
        report_lines.append("=" * 80)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"✓ Summary report saved to {output_path}")
        return output_path

