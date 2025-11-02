# IPTU Data Pipeline - Neuro Tech Challenge

Pipeline completo de processamento, validação e análise de dados IPTU da cidade de Recife.

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Requisitos](#requisitos)
- [Instalação](#instalação)
- [Uso](#uso)
- [Funcionalidades](#funcionalidades)
- [Arquitetura](#arquitetura)
- [Entregáveis](#entregáveis)

## 🎯 Visão Geral

Este projeto implementa um pipeline completo de dados para processar informações de IPTU (Imposto Predial e Territorial Urbano) da cidade de Recife, cobrindo os anos de 2020 a 2024. O pipeline inclui:

- ✅ Validação de qualidade dos dados
- ✅ Ingestão incremental de novos anos
- ✅ Transformação e unificação de schemas diferentes
- ✅ Análises estatísticas e de volume
- ✅ Dashboard interativo com visualizações
- ✅ Orquestração com Apache Airflow

## 📁 Estrutura do Projeto

```
neuro_tech/
├── data/                          # Dados de entrada
│   ├── iptu_2020/
│   ├── iptu_2021/
│   ├── iptu_2022/
│   ├── iptu_2023/
│   └── iptu_2024_json/
├── src/
│   └── iptu_pipeline/
│       ├── pipelines/
│       │   ├── ingestion.py      # Módulo de ingestão
│       │   ├── transformation.py # Módulo de transformação
│       │   ├── analysis.py       # Módulo de análises
│       │   └── main_pipeline.py  # Pipeline principal
│       ├── utils/
│       │   ├── data_quality.py   # Validação de qualidade
│       │   └── logger.py         # Sistema de logs
│       ├── config.py             # Configurações
│       ├── orchestration.py      # Orquestração Prefect
│       └── dashboard.py          # Geração de dashboard
├── notebooks/
│   ├── eda_notebook.ipynb        # Análise exploratória
│   └── pipeline_execution.ipynb  # Execução do pipeline
├── outputs/                      # Saídas do pipeline
│   ├── analyses/                 # Resultados das análises
│   ├── dashboard.html            # Dashboard interativo
│   └── *.csv                     # Relatórios diversos
├── logs/                         # Logs do pipeline
├── main.py                       # Script principal
├── pyproject.toml                # Dependências
├── ARCHITECTURE.md               # Diagrama de arquitetura
└── README.md                     # Este arquivo
```

## 🔧 Requisitos

- Python 3.11 ou superior
- uv (gerenciador de pacotes) - opcional, pode usar pip/conda

### Engines Suportados

O pipeline suporta dois engines de processamento:
- **Pandas** (padrão) - Para datasets até ~50 GB
- **PySpark** (opcional) - Para datasets grandes e processamento distribuído

Consulte `README_ENGINE.md` para detalhes sobre escolha e configuração do engine.

## 📦 Instalação

1. Clone o repositório ou navegue até o diretório do projeto

2. Instale as dependências:

```bash
# Com uv
uv sync

# Ou com pip
pip install -e .
```

## 🚀 Uso

### Execução Completa do Pipeline

**Opção 1: Local com Pandas (padrão)**
```bash
python main.py
```

**Opção 2: Local com PySpark**
```bash
export IPTU_DATA_ENGINE=pyspark  # Windows: $env:IPTU_DATA_ENGINE="pyspark"
python main.py
```

**Opção 3: Docker com Spark (Recomendado para demonstração)**
```bash
# Standalone mode (mais simples)
docker-compose -f docker-compose.standalone.yml up --build

# Ou usando script
# Windows PowerShell
.\docker-run.ps1

# Linux/Mac
chmod +x docker-run.sh && ./docker-run.sh
```

**Acesso ao Spark UI:** http://localhost:4040

Para mais detalhes sobre Docker, veja [DOCKER_SETUP.md](DOCKER_SETUP.md)

Ou através do Python:

```python
from iptu_pipeline.orchestration import run_orchestrated_pipeline

# Executar pipeline completo
consolidated_df = run_orchestrated_pipeline(
    years=None,  # None = todos os anos
    incremental=False,
    run_analysis=True
)
```

### Execução Incremental (Novos Anos)

Para adicionar apenas novos anos sem reprocessar tudo:

```python
from iptu_pipeline.orchestration import run_orchestrated_pipeline

# Apenas ano 2024 (exemplo)
consolidated_df = run_orchestrated_pipeline(
    years=[2024],
    incremental=True,  # Modo incremental
    run_analysis=True
)
```

### Geração de Dashboard

```python
from iptu_pipeline.dashboard import IPTUDashboard
import pandas as pd
from iptu_pipeline.config import CONSOLIDATED_DATA_PATH

# Carregar dados consolidados
df = pd.read_parquet(CONSOLIDATED_DATA_PATH)

# Criar e gerar dashboard
dashboard = IPTUDashboard(df=df)
dashboard.generate_dashboard_html()  # Gera outputs/dashboard.html
dashboard.generate_summary_report()  # Gera outputs/summary_report.txt
```

### Notebooks Jupyter

Execute os notebooks em `notebooks/` para:
- Análise exploratória inicial (`eda_notebook.ipynb`)
- Execução do pipeline (`pipeline_execution.ipynb`)

## ✨ Funcionalidades

### 1. Qualidade dos Dados

O módulo `DataQualityValidator` implementa validações robustas:

- ✅ Validação de estrutura (linhas, colunas)
- ✅ Verificação de valores nulos (% de threshold)
- ✅ Detecção de duplicatas
- ✅ Validação de tipos de dados
- ✅ Regras de negócio (CEP, cidade, estado)
- ✅ Geração de relatórios detalhados

**Saídas:**
- `outputs/validation_report.csv`: Resumo das validações por ano
- `outputs/validation_errors.csv`: Tabela detalhada de erros e warnings

### 2. Ingestão Incremental

O módulo `DataIngestion` suporta:

- ✅ Carregamento de CSV (2020-2023) e JSON (2024)
- ✅ Validação automática durante ingestão
- ✅ Modo incremental: processa apenas anos novos
- ✅ Status de ingestão por ano

**Benefícios:**
- Adiciona novos anos sem reprocessar toda a base
- Mantém histórico preservado
- Eficiente em termos de processamento

### 3. Tratamento e Unificação

O módulo `DataTransformer`:

- ✅ Normaliza diferenças de schema entre anos
- ✅ Trata coluna `_id` do ano 2024
- ✅ Padroniza nomes de colunas
- ✅ Adiciona colunas faltantes com valores nulos
- ✅ Padroniza tipos de dados
- ✅ Otimiza uso de memória (categorias, downcast)
- ✅ Consolida todos os anos em um dataset unificado

**Resultado:** Dataset unificado pronto para análise

### 4. Análises

O módulo `IPTUAnalyzer` realiza:

#### Análises Obrigatórias:

1. **Volume Total:**
   - Total de imóveis
   - Distribuição por ano (histórico)
   - Distribuição por tipo de uso
   - Distribuição por bairro
   - Volume por ano E tipo (cruzamento)
   - Volume por ano E bairro (cruzamento)

2. **Distribuição Física:**
   - Por tipo de uso (histórico)
   - Por bairro (histórico e top 20)
   - Por ano (distribuição temporal)
   - Top bairros por ano

#### Análise Adicional:

3. **Tendências de Valores de IPTU:**
   - Estatísticas de IPTU por ano (média, mediana, min, max)
   - Bairros com maiores valores médios de IPTU
   - Análise de valor total dos imóveis por ano

**Saídas:** CSV em `outputs/analyses/` organizados por tipo de análise

### 5. Orquestração

Orquestração implementada com **Apache Airflow**:

- ✅ DAG com tasks separadas para cada etapa
- ✅ Retry automático configurável
- ✅ Logging integrado por task
- ✅ Dependências claras entre tasks
- ✅ Execução paralela de validações
- ✅ Agendamento flexível

**Fluxo de Tasks:**
1. `validate_data_quality_2020-2024` (paralelas)
2. `ingest_data`
3. `transform_and_consolidate`
4. `save_consolidated_data`
5. `run_analysis`
6. `generate_dashboard`
7. `generate_reports`

Consulte `AIRFLOW_SETUP.md` para instruções de configuração e uso.

### 6. Visualizações e Gráficos

Gráficos estáticos gerados com **Matplotlib e Seaborn**:

- ✅ Volume de imóveis por ano (gráfico de barras)
- ✅ Distribuição por tipo de uso (pizza e barras)
- ✅ Top bairros por volume (gráfico horizontal)
- ✅ Evolução por ano e tipo (área empilhada)
- ✅ Tendências de valores de IPTU (linha e barras)
- ✅ Top bairros por valor de IPTU (gráfico horizontal)
- ✅ Distribuição por tipo de construção
- ✅ Distribuição temporal (linha)

**Saídas:**
- Gráficos PNG em alta resolução (`outputs/plots/`)
- Relatório HTML com todos os gráficos (`outputs/plots/visualizations_report.html`)

Para gerar os gráficos:
```python
from iptu_pipeline.visualizations import generate_plots_from_analysis_results

plot_files = generate_plots_from_analysis_results()
```

Ou execute o script:
```bash
python scripts/generate_plots.py
```

### 7. Dashboard e Relatórios

Dashboard interativo gerado com **Plotly**:

- ✅ Gráficos de volume por ano
- ✅ Distribuição por tipo (pie chart)
- ✅ Top bairros (bar charts)
- ✅ Heatmaps de distribuição
- ✅ Tendências de valores de IPTU
- ✅ Exportação para HTML interativo

**Saídas:**
- `outputs/dashboard.html`: Dashboard completo interativo
- `outputs/summary_report.txt`: Relatório textual resumido

## 🏗️ Arquitetura

Consulte `ARCHITECTURE.md` para:
- Diagrama completo da arquitetura
- Fluxo de dados detalhado
- Decisões de design
- Tecnologias utilizadas

### Resumo da Arquitetura

```
Dados → Ingestão → Validação → Transformação → Armazenamento → Análise → Dashboard
```

## 📊 Entregáveis

### 1. ✅ Código Desenvolvido

- Pipeline modular em Python
- Notebooks Jupyter para exploração e execução
- Scripts de orquestração

### 2. ✅ Diagrama de Arquitetura

- Arquivo `ARCHITECTURE.md` com:
  - Diagrama ASCII da arquitetura
  - Fluxo de dados detalhado
  - Tecnologias e decisões

### 3. ✅ Repositório Git

- Estrutura organizada
- Código modular
- Documentação completa

### 4. ✅ Dashboard/Relatório

- Dashboard HTML interativo (`outputs/dashboard.html`)
- Relatório textual (`outputs/summary_report.txt`)
- Análises em CSV (`outputs/analyses/`)

### 5. ✅ Materiais de Apoio

- Validações detalhadas (`outputs/validation_*.csv`)
- Notebooks de exploração
- Logs estruturados (`logs/`)

## 📈 Resultados das Análises

Após executar o pipeline, as análises respondem:

1. **Volume Total:** Total de imóveis e distribuição por tipo/bairro
2. **Histórico:** Evolução ano a ano
3. **Distribuição Física:** Top bairros, tipos de uso mais comuns
4. **Tendências:** Valores de IPTU ao longo do tempo (análise adicional)

## 🔍 Logs

Todos os logs são salvos em `logs/` com formato estruturado:
- Data e hora
- Nível de log
- Módulo origem
- Mensagem detalhada

## 📝 Notas

- O pipeline suporta adição de novos anos sem reprocessar toda a base
- Validações são registradas em arquivos CSV para auditoria
- Dashboard é gerado automaticamente após análises
- Airflow é opcional (pipeline funciona em modo direto sem Airflow)

## 🚧 Próximos Passos (Melhorias Futuras)

- [ ] Testes unitários automatizados
- [ ] CI/CD pipeline
- [ ] Deploy do dashboard em servidor web
- [ ] Integração com banco de dados
- [ ] Agendamento automático com Prefect Cloud
- [ ] Alertas de qualidade de dados

## 📄 Licença

Este projeto foi desenvolvido para o desafio técnico da Neuro Tech.

