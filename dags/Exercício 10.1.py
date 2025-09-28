from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models import Variable
from airflow.exceptions import AirflowException
import pendulum
import pandas as pd
import requests
from datetime import date, datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Variáveis configuráveis
PROJECT_GCP   = Variable.get("GCP_PROJECT_ID", default_var="951374833974")
DATASET_BQ    = Variable.get("BIGQUERY_DATASET", default_var="dataset_medicamentos_fda")
TABELA_BQ     = Variable.get("BIGQUERY_TABLE", default_var="eventos_medicamentos_periodo")
LOCALIZACAO_BQ = Variable.get("BIGQUERY_LOCATION", default_var="US")
CONEXAO_GCP_ID = Variable.get("GCP_CONNECTION_ID", default_var="google_cloud_default")
USAR_POOL     = Variable.get("USE_POOL", default_var=True, deserialize_json=True)
NOME_POOL     = Variable.get("POOL_NAME", default_var="api_medicamentos_pool")
DATA_INICIO   = Variable.get("START_DATE", default_var="2025-01-01")
DATA_FIM      = Variable.get("END_DATE", default_var="2025-12-31")
MEDICAMENTO   = Variable.get("MEDICATION_QUERY", default_var="sildenafil+citrate")

# Conversão de datas
DATA_INICIO = datetime.strptime(DATA_INICIO, "%Y-%m-%d").date()
DATA_FIM = datetime.strptime(DATA_FIM, "%Y-%m-%d").date()

def criar_sessao_http():
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    sessao = requests.Session()
    sessao.mount("https://", adapter)
    sessao.headers.update({
        "User-Agent": "pipeline-medicamentos-fda/2.0",
        "Accept": "application/json"
    })
    return sessao

def construir_url(inicio: date, fim: date, medicamento: str) -> str:
    med = requests.utils.quote(medicamento.strip())
    di = inicio.strftime("%Y%m%d")
    df = fim.strftime("%Y%m%d")
    return f"https://api.fda.gov/drug/event.json?search=patient.drug.medicinalproduct:%22{med}%22+AND+receivedate:[{di}+TO+{df}]&count=receivedate"

@task(
    retries=2,
    retry_delay=pendulum.duration(minutes=5),
    pool=NOME_POOL if USAR_POOL else None,
    retry_exponential_backoff=True,
    max_retry_delay=pendulum.duration(minutes=30)
)
def extrair_processar_carregar():
    sessao = criar_sessao_http()
    url = construir_url(DATA_INICIO, DATA_FIM, MEDICAMENTO)
    resp = sessao.get(url, timeout=30)

    if resp.status_code == 404:
        return

    resp.raise_for_status()
    dados = resp.json().get("results", [])
    if not dados:
        return

    df = pd.DataFrame(dados).rename(columns={"count": "total_eventos", "time": "data_evento_str"})
    df["data_evento"] = pd.to_datetime(df["data_evento_str"], format="%Y%m%d", errors="coerce", utc=True)
    df = df.dropna(subset=["data_evento"])

    df["periodo_inicio"] = pd.to_datetime(DATA_INICIO)
    df["periodo_fim"] = pd.to_datetime(DATA_FIM)
    df["medicamento_analisado"] = MEDICAMENTO.replace("+", " ")
    df["data_processamento"] = pd.Timestamp.now(tz="UTC")

    df = df[[
        "data_evento", "total_eventos", "periodo_inicio",
        "periodo_fim", "medicamento_analisado", "data_processamento"
    ]]

    hook_bq = BigQueryHook(
        gcp_conn_id=CONEXAO_GCP_ID,
        location=LOCALIZACAO_BQ,
        use_legacy_sql=False
    )

    schema = [
        {"name": "data_evento", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "total_eventos", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "periodo_inicio", "type": "DATE", "mode": "REQUIRED"},
        {"name": "periodo_fim", "type": "DATE", "mode": "REQUIRED"},
        {"name": "medicamento_analisado", "type": "STRING", "mode": "REQUIRED"},
        {"name": "data_processamento", "type": "TIMESTAMP", "mode": "REQUIRED"}
    ]

    df.to_gbq(
        destination_table=f"{DATASET_BQ}.{TABELA_BQ}",
        project_id=PROJECT_GCP,
        if_exists="append",
        credentials=hook_bq.get_credentials(),
        table_schema=schema,
        location=LOCALIZACAO_BQ,
        progress_bar=False
    )

default_args = {"owner": "Jeferson"}

@dag(
    dag_id="pipeline_medicamentos_fda_simplificado",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["fda", "medicamentos", "bigquery"]
)
def dag_pipeline_medicamentos():
    extrair_processar_carregar()

dag = dag_pipeline_medicamentos()
