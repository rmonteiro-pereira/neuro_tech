# Diagrama de Arquitetura - IPTU Data Pipeline

## Visão Geral da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (Origem dos Dados)                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │ IPTU 2020   │  │ IPTU 2021   │  │ IPTU 2022   │  │ IPTU 2023   │ │
│  │   (CSV)     │  │   (CSV)     │  │   (CSV)     │  │   (CSV)     │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │
│                                                                         │
│  ┌─────────────┐                                                       │
│  │ IPTU 2024   │                                                       │
│  │   (JSON)    │                                                       │
│  └─────────────┘                                                       │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION LAYER (Orquestração)                    │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │                 Apache Airflow Orchestrator                     │  │
│  │                                                                  │  │
│  │  ┌──────────────────────────────────────────────────────────┐  │  │
│  │  │              DAG: iptu_data_pipeline                     │  │  │
│  │  │  - Task: validate_data_quality (parallel by year)        │  │  │
│  │  │  - Task: ingest_data                                      │  │  │
│  │  │  - Task: transform_and_consolidate                        │  │  │
│  │  │  - Task: save_consolidated_data                           │  │  │
│  │  │  - Task: run_analysis                                     │  │  │
│  │  │  - Task: generate_dashboard                               │  │  │
│  │  │  - Task: generate_reports                                 │  │  │
│  │  └──────────────────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    PROCESSING PIPELINE (Pipeline de Processamento)      │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  STEP 1: Data Ingestion (Ingestão de Dados)                    │  │
│  │  ┌──────────────────────────────────────────────────────────┐  │  │
│  │  │  DataIngestion Class                                      │  │  │
│  │  │  - load_csv_file()      → CSV files (2020-2023)          │  │  │
│  │  │  - load_json_file()     → JSON file (2024)              │  │  │
│  │  │  - load_incremental()   → Incremental loading            │  │  │
│  │  └──────────────────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                  │                                     │
│                                  ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  STEP 2: Data Quality Validation (Validação de Qualidade)       │  │
│  │  ┌──────────────────────────────────────────────────────────┐  │  │
│  │  │  DataQualityValidator Class                               │  │  │
│  │  │  - validate_structure()    → Basic structure checks       │  │  │
│  │  │  - validate_row_count()   → Minimum row threshold        │  │  │
│  │  │  - validate_columns()     → Required columns check       │  │  │
│  │  │  - validate_null_values()  → Null percentage check        │  │  │
│  │  │  - validate_duplicates()   → Duplicate detection          │  │  │
│  │  │  - validate_business_rules() → Business logic checks     │  │  │
│  │  │                                                             │  │  │
│  │  │  Output: validation_report.csv, validation_errors.csv     │  │  │
│  │  └──────────────────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                  │                                     │
│                                  ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  STEP 3: Data Transformation (Transformação e Unificação)      │  │
│  │  ┌──────────────────────────────────────────────────────────┐  │  │
│  │  │  DataTransformer Class                                    │  │  │
│  │  │  - normalize_column_names()   → Schema alignment           │  │  │
│  │  │  - handle_missing_columns()   → Add missing columns       │  │  │
│  │  │  - standardize_data_types()   → Type standardization      │  │  │
│  │  │  - clean_and_optimize()       → Memory optimization       │  │  │
│  │  │  - consolidate_datasets()     → Merge all years           │  │  │
│  │  └──────────────────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                  │                                     │
│                                  ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  STEP 4: Data Storage (Armazenamento)                           │  │
│  │                                                                  │  │
│  │  ┌────────────────────┐    ┌────────────────────┐              │  │
│  │  │ iptu_consolidated. │    │ iptu_processed.   │              │  │
│  │  │    parquet         │    │    parquet         │              │  │
│  │  │  (All years merged)│    │  (Optimized)       │              │  │
│  │  └────────────────────┘    └────────────────────┘              │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                  │                                     │
│                                  ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  STEP 5: Data Analysis (Análise de Dados)                       │  │
│  │  ┌──────────────────────────────────────────────────────────┐  │  │
│  │  │  IPTUAnalyzer Class                                       │  │  │
│  │  │  - analyze_volume_total()      → Total volume analysis   │  │  │
│  │  │  - analyze_distribution_physical() → Distribution        │  │  │
│  │  │  - analyze_tax_value_trends()   → Tax trends (extra)     │  │  │
│  │  │                                                             │  │  │
│  │  │  Output: CSV files in outputs/analyses/                   │  │  │
│  │  └──────────────────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    OUTPUT LAYER (Camada de Saída)                       │
│                                                                         │
│  ┌────────────────────┐  ┌────────────────────┐  ┌─────────────────┐ │
│  │   Dashboard HTML   │  │  Summary Report   │  │ Analysis CSVs   │ │
│  │  (Interactive)      │  │  (Text)            │  │ (All analyses)  │ │
│  └────────────────────┘  └────────────────────┘  └─────────────────┘ │
│                                                                         │
│  ┌────────────────────┐  ┌────────────────────┐                      │
│  │ Validation Report  │  │ Validation Errors  │                      │
│  │   (CSV)            │  │      (CSV)         │                      │
│  └────────────────────┘  └────────────────────┘                      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Fluxo de Dados Detalhado

### 1. Ingestão (Ingestion)
- **Input**: Arquivos CSV (2020-2023) e JSON (2024)
- **Processo**: 
  - Detecta formato do arquivo
  - Carrega dados em DataFrames
  - Valida estrutura básica
- **Output**: Dicionário {ano: DataFrame}

### 2. Validação de Qualidade (Quality Validation)
- **Input**: DataFrames por ano
- **Processo**:
  - Valida estrutura (linhas, colunas)
  - Verifica valores nulos
  - Detecta duplicatas
  - Valida regras de negócio (CEP, cidade, estado)
- **Output**: Relatórios de validação e tabela de erros

### 3. Transformação (Transformation)
- **Input**: DataFrames individuais por ano
- **Processo**:
  - Normaliza nomes de colunas (especialmente 2024)
  - Adiciona colunas faltantes
  - Padroniza tipos de dados
  - Otimiza uso de memória
  - Consolida todos os anos
- **Output**: DataFrame unificado

### 4. Análise (Analysis)
- **Input**: DataFrame consolidado
- **Processo**:
  - Análise de volume (total, por ano, por tipo, por bairro)
  - Análise de distribuição física
  - Análise de tendências de valores (análise adicional)
- **Output**: Múltiplos CSVs com resultados

### 5. Visualização (Dashboard)
- **Input**: Resultados das análises
- **Processo**:
  - Gera gráficos interativos (Plotly)
  - Cria dashboard HTML
  - Gera relatório resumido
- **Output**: Dashboard HTML e relatório texto

## Tecnologias Utilizadas

- **Python 3.11+**: Linguagem principal
- **Pandas**: Manipulação e análise de dados
- **PyArrow**: Formato Parquet para armazenamento eficiente
- **Apache Airflow**: Orquestração de workflows
- **Plotly**: Visualizações interativas
- **Logging**: Sistema de logs estruturado

## Decisões de Arquitetura

### 1. Modularidade
- Cada etapa do pipeline é um módulo independente
- Fácil manutenção e extensão
- Testes unitários facilitados

### 2. Ingestão Incremental
- Suporta adição de novos anos sem reprocessar tudo
- Verifica anos existentes no dataset consolidado
- Processa apenas dados novos

### 3. Validação Robusta
- Múltiplas camadas de validação
- Relatórios detalhados de erros e warnings
- Logs estruturados para rastreabilidade

### 4. Otimização de Memória
- Conversão de tipos otimizada
- Uso de categorias para colunas com poucos valores únicos
- Armazenamento em Parquet (compressão)

### 5. Orquestração com Apache Airflow
- DAG declarativo com dependências entre tasks
- Retry automático configurável
- Visualização de dependências na UI
- Logging integrado por task
- Agendamento flexível (diário, semanal, etc.)
- Execução paralela de validações por ano

## Fluxo Incremental

Quando executado em modo incremental:

```
1. Verifica anos existentes no iptu_consolidated.parquet
2. Identifica anos novos a processar
3. Carrega apenas anos novos
4. Valida anos novos
5. Transforma anos novos
6. Anexa ao dataset existente
7. Atualiza análises (opcional)
```

Isso permite adicionar novos anos sem reprocessar toda a base histórica.

