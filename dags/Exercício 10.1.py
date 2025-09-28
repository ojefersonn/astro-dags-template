from __future__ import annotations
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum
import pandas as pd
import requests
from datetime import date

# Configurações do Pipeline de Dados Farmacêuticos
PROJECT_GCP = "951374833974"
DATASET_BQ = "dataset_medicamentos_fda"
TABELA_BQ = "eventos_medicamentos_periodo"
LOCALIZACAO_BQ = "US"
CONEXAO_GCP_ID = "google_cloud_default"
USAR_POOL = True
NOME_POOL = "api_medicamentos_pool"
DATA_INICIO_ANALISE = date(2025, 1, 1)
DATA_FIM_ANALISE = date(2025, 12, 31)
MEDICAMENTO_CONSULTA = 'sildenafil+citrate'

# Configuração da sessão HTTP para requisições à API
sessao_http = requests.Session()
sessao_http.headers.update({
    "User-Agent": "pipeline-medicamentos-fda/2.0 (analise.dados@institucao.gov.br)"
})

def obter_dados_api_fda(endpoint_url: str) -> dict:
    """
    Realiza requisição à API do FDA e retorna dados em formato JSON

    Args:
        endpoint_url: URL completa para consulta à API

    Returns:
        dict: Dados da resposta em formato JSON
    """
    resposta = sessao_http.get(endpoint_url, timeout=30)
    if resposta.status_code == 404:
        return {"results": []}
    resposta.raise_for_status()
    return resposta.json()

def construir_url_consulta_fda(inicio: date, fim: date, medicamento: str) -> str:
    """
    Constrói URL para consulta de eventos de medicamentos na API FDA

    Args:
        inicio: Data de início do período
        fim: Data de fim do período
        medicamento: Nome do medicamento para consulta

    Returns:
        str: URL formatada para a API
    """
    data_inicio_fmt = inicio.strftime("%Y%m%d")
    data_fim_fmt = fim.strftime("%Y%m%d")

    url_base = "https://api.fda.gov/drug/event.json"
    parametros_busca = f"?search=patient.drug.medicinalproduct:%22{medicamento}%22"
    filtro_data = f"+AND+receivedate:[{data_inicio_fmt}+TO+{data_fim_fmt}]"
    agregacao = "&count=receivedate"

    return url_base + parametros_busca + filtro_data + agregacao

# Configurações da task do Airflow
configuracoes_task = dict(retries=2, retry_delay=pendulum.duration(minutes=5))
if USAR_POOL:
    configuracoes_task["pool"] = NOME_POOL

@task(**configuracoes_task)
def processar_dados_periodo_fixo():
    """
    Extrai dados de eventos farmacêuticos da API FDA e carrega no BigQuery
    """
    url_consulta = construir_url_consulta_fda(
        DATA_INICIO_ANALISE, 
        DATA_FIM_ANALISE, 
        MEDICAMENTO_CONSULTA
    )

    dados_resposta = obter_dados_api_fda(url_consulta)
    eventos_encontrados = dados_resposta.get("results", [])

    if not eventos_encontrados:
        print("Nenhum evento encontrado no período especificado")
        return

    # Processamento dos dados
    df_eventos = pd.DataFrame(eventos_encontrados)
    df_eventos = df_eventos.rename(columns={"count": "total_eventos"})

    # Conversões de data e enriquecimento dos dados
    df_eventos["data_evento"] = pd.to_datetime(
        df_eventos["time"], 
        format="%Y%m%d", 
        utc=True
    )
    df_eventos["periodo_inicio"] = pd.to_datetime(DATA_INICIO_ANALISE)
    df_eventos["periodo_fim"] = pd.to_datetime(DATA_FIM_ANALISE)
    df_eventos["medicamento_analisado"] = MEDICAMENTO_CONSULTA.replace("+", " ")
    df_eventos["data_processamento"] = pd.Timestamp.now(tz="UTC")

    # Conexão e carregamento no BigQuery
    hook_bq = BigQueryHook(
        gcp_conn_id=CONEXAO_GCP_ID, 
        location=LOCALIZACAO_BQ, 
        use_legacy_sql=False
    )

    # Schema da tabela no BigQuery
    schema_tabela = [
        {"name": "data_evento", "type": "TIMESTAMP"},
        {"name": "total_eventos", "type": "INTEGER"},
        {"name": "periodo_inicio", "type": "DATE"},
        {"name": "periodo_fim", "type": "DATE"},
        {"name": "medicamento_analisado", "type": "STRING"},
        {"name": "data_processamento", "type": "TIMESTAMP"}
    ]

    # Carregamento dos dados
    df_eventos.to_gbq(
        destination_table=f"{DATASET_BQ}.{TABELA_BQ}",
        project_id=PROJECT_GCP,
        if_exists="append",
        credentials=hook_bq.get_credentials(),
        table_schema=schema_tabela,
        location=LOCALIZACAO_BQ,
        progress_bar=False
    )

    print(f"Processados {len(df_eventos)} registros de eventos farmacêuticos")

@dag(
    dag_id="pipeline_analise_medicamentos_fda_v2",
    description="Pipeline para análise de eventos farmacêuticos via API FDA",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["medicamentos", "fda", "bigquery", "analise_eventos", "farmaceuticos"],
    doc_md="""
    # Pipeline de Análise de Eventos Farmacêuticos

    Este DAG realiza a extração de dados de eventos farmacêuticos da API do FDA
    e carrega os dados processados no BigQuery para análises posteriores.

    ## Funcionalidades:
    - Consulta à API FDA para eventos de medicamentos específicos
    - Processamento e enriquecimento dos dados
    - Carregamento no BigQuery com schema definido

    ## Configurações:
    - Período de análise: 2025
    - Medicamento: Sildenafil Citrate
    - Destino: BigQuery dataset medicamentos_fda
    """
)
def dag_pipeline_medicamentos():
    """
    DAG principal para processamento de dados farmacêuticos
    """
    processar_dados_periodo_fixo()

# Instanciação do DAG
dag_medicamentos = dag_pipeline_medicamentos()
