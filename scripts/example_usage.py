"""
Exemplo de uso do IPTU Data Pipeline.

Este script demonstra diferentes formas de usar o pipeline.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from iptu_pipeline.orchestration import run_orchestrated_pipeline
from iptu_pipeline.dashboard import IPTUDashboard
from iptu_pipeline.config import CONSOLIDATED_DATA_PATH
import pandas as pd


def example_full_pipeline():
    """Exemplo 1: Executar pipeline completo para todos os anos."""
    print("="*80)
    print("Exemplo 1: Pipeline Completo")
    print("="*80)
    
    consolidated_df = run_orchestrated_pipeline(
        years=None,  # None = processa todos os anos disponíveis
        incremental=False,  # False = processa tudo do zero
        run_analysis=True  # True = executa análises automaticamente
    )
    
    print(f"\n✓ Pipeline completo executado com sucesso!")
    print(f"  Dataset consolidado: {len(consolidated_df):,} linhas, {len(consolidated_df.columns)} colunas")
    return consolidated_df


def example_incremental_pipeline():
    """Exemplo 2: Pipeline incremental - adiciona apenas novos anos."""
    print("\n" + "="*80)
    print("Exemplo 2: Pipeline Incremental")
    print("="*80)
    
    # Suponha que você já tem dados até 2023 e quer adicionar 2024
    new_years = [2024]
    
    consolidated_df = run_orchestrated_pipeline(
        years=new_years,
        incremental=True,  # True = modo incremental
        run_analysis=True
    )
    
    print(f"\n✓ Pipeline incremental executado com sucesso!")
    print(f"  Apenas ano(s) {new_years} processado(s)")
    return consolidated_df


def example_dashboard_generation():
    """Exemplo 3: Gerar dashboard a partir de dados consolidados."""
    print("\n" + "="*80)
    print("Exemplo 3: Geração de Dashboard")
    print("="*80)
    
    # Carregar dados consolidados
    if not CONSOLIDATED_DATA_PATH.exists():
        print("Erro: Dados consolidados não encontrados. Execute o pipeline primeiro.")
        return
    
    df = pd.read_parquet(CONSOLIDATED_DATA_PATH)
    print(f"Carregado dataset com {len(df):,} linhas")
    
    # Criar dashboard
    dashboard = IPTUDashboard(df=df)
    
    # Gerar dashboard HTML
    dashboard_path = dashboard.generate_dashboard_html()
    print(f"\n✓ Dashboard gerado: {dashboard_path}")
    
    # Gerar relatório resumido
    report_path = dashboard.generate_summary_report()
    print(f"✓ Relatório gerado: {report_path}")
    
    return dashboard_path, report_path


def example_specific_years():
    """Exemplo 4: Processar apenas anos específicos."""
    print("\n" + "="*80)
    print("Exemplo 4: Processar Anos Específicos")
    print("="*80)
    
    # Processar apenas 2023 e 2024
    years = [2023, 2024]
    
    consolidated_df = run_orchestrated_pipeline(
        years=years,
        incremental=False,
        run_analysis=True
    )
    
    print(f"\n✓ Processados apenas anos {years}")
    return consolidated_df


if __name__ == "__main__":
    print("\n" + "="*80)
    print("IPTU Data Pipeline - Exemplos de Uso")
    print("="*80)
    
    # Descomente o exemplo que deseja executar:
    
    # Exemplo 1: Pipeline completo
    # example_full_pipeline()
    
    # Exemplo 2: Pipeline incremental
    # example_incremental_pipeline()
    
    # Exemplo 3: Gerar dashboard (requer dados consolidados)
    # example_dashboard_generation()
    
    # Exemplo 4: Anos específicos
    # example_specific_years()
    
    print("\n" + "="*80)
    print("Para executar um exemplo, descomente a função correspondente acima.")
    print("="*80)

