dfrom __future__ import annotations
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum
import pandas as pd
import requests
from datetime import date

# Configurações
GCP_PROJECT  = "951374833974"
BQ_DATASET   = "dataset_fda"
BQ_TABLE     = "openfda_electronic_cigarette_range_test"
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"
USE_POOL     = True
POOL_NAME    = "openfda_api"
TEST_START = date(2020, 1, 1)  # Período mais amplo
TEST_END   = date(2024, 12, 31)
TOBACCO_TERM = 'electronic cigarette'  # Sem + e aspas

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "mda-openfda-etl/1.0 (contato: voce@exemplo.gov.br)"})

def _openfda_get(url: str) -> dict:
    r = SESSION.get(url, timeout=30)
    if r.status_code == 404:
        return {"results": []}
    r.raise_for_status()
    return r.json()

def _build_openfda_tobacco_url(start: date, end: date, tobacco_term: str) -> str:
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")
    # Query corrigida para a estrutura real da API tobacco
    return (f"https://api.fda.gov/tobacco/problem.json"
            f"?search=tobacco_products.tobacco_product_name:{tobacco_term.replace(' ', '+')}"
            f"+AND+date_submitted:[{start_str}+TO+{end_str}]"
            "&limit=1000")  # Busca registros individuais, não count

_task_kwargs = dict(retries=0)
if USE_POOL:
    _task_kwargs["pool"] = POOL_NAME

@task(**_task_kwargs)
def fetch_tobacco_data_to_bq():
    url = _build_openfda_tobacco_url(TEST_START, TEST_END, TOBACCO_TERM)
    print(f"🔍 URL: {url}")
    
    data = _openfda_get(url)
    results = data.get("results", [])
    
    print(f"📊 Registros encontrados: {len(results)}")
    
    if not results:
        print("⚠️ Nenhum resultado encontrado")
        return
    
    # Processa dados tobacco (estrutura diferente!)
    processed_data = []
    
    for record in results:
        # Extrai dados do registro tobacco
        processed_data.append({
            "report_id": record.get("report_id", ""),
            "date_submitted": record.get("date_submitted", ""),
            "product_problems": str(record.get("reported_product_problems", [])),
            "health_problems": str(record.get("reported_health_problems", [])),
            "tobacco_products": str(record.get("tobacco_products", [])),
            "product_search": TOBACCO_TERM
        })
    
    df = pd.DataFrame(processed_data)
    
    # Converte data
    df["date_submitted"] = pd.to_datetime(df["date_submitted"], format="%Y%m%d", errors='coerce', utc=True)
    df["win_start"] = pd.to_datetime(TEST_START)
    df["win_end"] = pd.to_datetime(TEST_END)
    
    print(f"✅ DataFrame criado: {df.shape}")
    print(f"Colunas: {df.columns.tolist()}")
    
    # Grava no BigQuery
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=bq_hook.get_credentials(),
        table_schema=[
            {"name": "report_id", "type": "STRING"},
            {"name": "date_submitted", "type": "TIMESTAMP"},
            {"name": "product_problems", "type": "STRING"},
            {"name": "health_problems", "type": "STRING"},
            {"name": "tobacco_products", "type": "STRING"},
            {"name": "product_search", "type": "STRING"},
            {"name": "win_start", "type": "DATE"},
            {"name": "win_end", "type": "DATE"}
        ],
        location=BQ_LOCATION,
        progress_bar=False
    )
    
    print("🎉 Dados gravados no BigQuery com sucesso!")

@dag(dag_id="openfda_tobacco_reports_fixed",
     default_args={"owner": "Jeferson"},
     schedule="@once",
     start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
     catchup=False,
     max_active_runs=1,
     tags=["openfda", "bigquery", "tobacco", "reports"])
def openfda_tobacco_pipeline():
    fetch_tobacco_data_to_bq()

dag = openfda_tobacco_pipeline()
