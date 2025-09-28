from __future__ import annotations
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum, pandas as pd, requests, time, json
from datetime import date
from urllib.parse import quote_plus
from typing import Optional

# ===== Config =====
GCP_PROJECT  = "951374833974"
BQ_DATASET   = "dataset_fda"
BQ_TABLE     = "openfda_electronic_cigarette_daily_counts"  # <— tabela de contagem por dia
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"

USE_POOL     = True
POOL_NAME    = "openfda_api"

TEST_START = date(2020, 1, 1)
TEST_END   = date(2024, 12, 31)
TOBACCO_TERM = 'electronic cigarette'

OPENFDA_BASE = "https://api.fda.gov/tobacco/problem.json"
REQUEST_TIMEOUT = 30
RETRY_TIMES = 5
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "mda-openfda-etl/1.0 (contato: voce@exemplo.gov.br)"})

# Opcional: configure uma API KEY no Airflow (Variable) para aumentar cotas
OPENFDA_API_KEY: Optional[str] = None
try:
    from airflow.models import Variable
    OPENFDA_API_KEY = Variable.get("OPENFDA_API_KEY", default_var=None)
except Exception:
    pass

def _openfda_get(url: str) -> dict:
    # backoff com Retry-After (quando presente)
    sleep_base = 1.5
    for attempt in range(1, RETRY_TIMES + 1):
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 404:
            return {"results": []}
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else (sleep_base ** attempt)
            time.sleep(min(wait, 30))
            continue
        if r.status_code >= 500 and attempt < RETRY_TIMES:
            time.sleep(sleep_base ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    # fallback
    r.raise_for_status()

def _build_search(start: date, end: date, term: str) -> str:
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")
    term_quoted = quote_plus(f'"{term}"')
    return f"tobacco_products.tobacco_product_name:{term_quoted}+AND+date_submitted:[{start_str}+TO+{end_str}]"

def _count_url(search: str) -> str:
    base = f"{OPENFDA_BASE}?search={search}&count=date_submitted"
    if OPENFDA_API_KEY:
        base += f"&api_key={OPENFDA_API_KEY}"
    return base

_task_kwargs = dict(retries=0)
if USE_POOL:
    _task_kwargs["pool"] = POOL_NAME

@task(**_task_kwargs)
def fetch_counts_to_bq():
    search = _build_search(TEST_START, TEST_END, TOBACCO_TERM)
    url = _count_url(search)
    print(f"🔍 URL (count): {url}")
    payload = _openfda_get(url)

    # openFDA retorna algo como [{"term": "2020-01-01", "count": 5}, ...]
    rows = payload if isinstance(payload, list) else payload.get("results", [])
    if not rows:
        print("⚠️ Sem resultados.")
        return

    df = pd.DataFrame(rows)
    # Normaliza colunas
    # 'term' pode vir 'YYYY-MM-DD' ou 'YYYYMMDD' dependendo do endpoint — tratamos os dois
    df["data"] = pd.to_datetime(df["term"], errors="coerce").dt.date
    df = df.rename(columns={"count": "qtde"})[["data", "qtde"]].dropna()

    # Adiciona metadados úteis (termo e janela) – opcionais
    df["product_search"] = TOBACCO_TERM
    df["win_start"] = TEST_START
    df["win_end"] = TEST_END

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=bq_hook.get_credentials(),
        table_schema=[
            {"name": "data",           "type": "DATE"},
            {"name": "qtde",           "type": "INT64"},
            {"name": "product_search", "type": "STRING"},
            {"name": "win_start",      "type": "DATE"},
            {"name": "win_end",        "type": "DATE"},
        ],
        location=BQ_LOCATION,
        progress_bar=False
    )
    print(f"✅ Gravado: {len(df)} linhas de contagem diária.")

@dag(
    dag_id="openfda_tobacco_counts",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "bigquery", "tobacco", "counts"]
)
def pipeline_counts():
    fetch_counts_to_bq()

dag = pipeline_counts()
