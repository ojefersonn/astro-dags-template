from __future__ import annotations
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum
import pandas as pd
import requests
from datetime import date
from urllib.parse import quote_plus
import json
import math
import time

# ================= Configurações =================
GCP_PROJECT  = "951374833974"
BQ_DATASET   = "dataset_fda"
BQ_TABLE     = "openfda_electronic_cigarette_range_test"
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"

USE_POOL     = True
POOL_NAME    = "openfda_api"

# Janela ampla para garantir amostragem
TEST_START = date(2020, 1, 1)
TEST_END   = date(2024, 12, 31)

# Termo de busca (com aspas na query para frase exata)
TOBACCO_TERM = 'electronic cigarette'

# API params
OPENFDA_BASE = "https://api.fda.gov/tobacco/problem.json"
LIMIT = 1000               # máx. por página segundo openFDA
MAX_PAGES = 30             # guarda-chuva: 30k registros; ajuste se necessário
REQUEST_TIMEOUT = 30
RETRY_TIMES = 3
RETRY_SLEEP = 2            # segundos

SESSION = requests.Session()
SESSION.headers.update(
    {"User-Agent": "mda-openfda-etl/1.0 (contato: voce@exemplo.gov.br)"}
)

def _openfda_get(url: str) -> dict:
    for attempt in range(1, RETRY_TIMES + 1):
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 404:
            return {"results": []}
        if r.status_code >= 500:
            if attempt < RETRY_TIMES:
                time.sleep(RETRY_SLEEP * attempt)
                continue
        r.raise_for_status()
        return r.json()
    # fallback
    return {"results": []}

def _build_query(start: date, end: date, tobacco_term: str) -> str:
    # date_submitted em formato YYYYMMDD
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")

    # Aspas para “phrase match” e URL-encode
    # Campo de produto (nome) + filtro por data
    term_quoted = quote_plus(f'"{tobacco_term}"')
    # Importante: +AND+ entre condições; intervalo em [start TO end]
    search = (
        f"tobacco_products.tobacco_product_name:{term_quoted}"
        f"+AND+date_submitted:[{start_str}+TO+{end_str}]"
    )
    return search

def _page_url(search: str, limit: int, skip: int) -> str:
    return f"{OPENFDA_BASE}?search={search}&limit={limit}&skip={skip}"

_task_kwargs = dict(retries=0)
if USE_POOL:
    _task_kwargs["pool"] = POOL_NAME

@task(**_task_kwargs)
def fetch_tobacco_data_to_bq():
    search = _build_query(TEST_START, TEST_END, TOBACCO_TERM)

    all_rows = []
    total_fetched = 0

    for page in range(MAX_PAGES):
        skip = page * LIMIT
        url = _page_url(search, LIMIT, skip)
        data = _openfda_get(url)
        results = data.get("results", []) or []

        if not results:
            break

        for record in results:
            # Campos esperados no endpoint “tobacco/problem”
            report_id = record.get("report_id", "")
            date_raw = record.get("date_submitted", None)  # geralmente 'YYYYMMDD'

            # Converte para datetime -> e depois DATE
            # Se vier nulo/ruim, vira None e será filtrável no Looker
            date_ts = pd.to_datetime(date_raw, format="%Y%m%d", errors="coerce", utc=True)
            date_only = date_ts.date() if pd.notna(date_ts) else None

            # Colunas de interesse (listas viram JSON string para evitar schema dinâmico)
            product_problems = json.dumps(record.get("reported_product_problems", []), ensure_ascii=False)
            health_problems  = json.dumps(record.get("reported_health_problems", []), ensure_ascii=False)
            tobacco_products = json.dumps(record.get("tobacco_products", []), ensure_ascii=False)

            # Campo mensal pronto p/ Looker (primeiro dia do mês)
            mes = None
            if date_only is not None:
                mes = pd.Timestamp(date_only).to_period("M").to_timestamp().date()

            all_rows.append({
                "report_id": str(report_id),
                "date_submitted": date_raw,               # original string (rastreamento)
                "date_submitted_date": date_only,         # DATE
                "mes": mes,                                # DATE (1º dia do mês)
                "product_problems": product_problems,      # STRING JSON
                "health_problems": health_problems,        # STRING JSON
                "tobacco_products": tobacco_products,      # STRING JSON
                "product_search": TOBACCO_TERM,            # termo usado
                "win_start": TEST_START,                   # DATE
                "win_end": TEST_END                        # DATE
            })

        total_fetched += len(results)
        if len(results) < LIMIT:
            # última página
            break

    if not all_rows:
        print("Nenhum resultado retornado dentro do período/termo informado.")
        return

    df = pd.DataFrame(all_rows)

    print(f"Total de registros coletados: {len(df)}")
    print(f"Colunas: {df.columns.tolist()}")

    # Grava no BigQuery com schema explícito
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)

    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=bq_hook.get_credentials(),
        table_schema=[
            {"name": "report_id",           "type": "STRING"},
            {"name": "date_submitted",      "type": "STRING"},   # original cru
            {"name": "date_submitted_date", "type": "DATE"},     # pronto p/ Looker
            {"name": "mes",                 "type": "DATE"},     # pronto p/ Looker (mensal)
            {"name": "product_problems",    "type": "STRING"},
            {"name": "health_problems",     "type": "STRING"},
            {"name": "tobacco_products",    "type": "STRING"},
            {"name": "product_search",      "type": "STRING"},
            {"name": "win_start",           "type": "DATE"},
            {"name": "win_end",             "type": "DATE"},
        ],
        location=BQ_LOCATION,
        progress_bar=False
    )

@dag(
    dag_id="openfda_tobacco_report",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "bigquery", "tobacco", "reports"]
)
def openfda_tobacco_pipeline():
    fetch_tobacco_data_to_bq()

dag = openfda_tobacco_pipeline()
