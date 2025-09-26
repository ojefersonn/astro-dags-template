from __future__ import annotations
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum
import pandas as pd
import requests
from datetime import date

# ===== Configurações =====
GCP_PROJECT   = "951374833974"
BQ_DATASET    = "enapdatasets"
BQ_TABLE      = "cvm_events_dog_ivermectin_daily"
BQ_LOCATION   = "US"
GCP_CONN_ID   = "google_cloud_default"

USE_POOL      = True
POOL_NAME     = "openfda_api"

TEST_START    = date(2025, 6, 1)   # período do exercício
TEST_END      = date(2025, 7, 29)

SPECIES       = "DOG"              # mude para "CAT", etc.
ACTIVE_ING    = "ivermectin"       # mude o ativo se quiser

# ===== HTTP session =====
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "mda-openfda-etl/1.0 (contato: voce@exemplo.gov.br)"})

def _openfda_get(url: str) -> dict:
    r = SESSION.get(url, timeout=30)
    if r.status_code == 404:
        return {"results": []}
    r.raise_for_status()
    return r.json()

def _build_openfda_url(start: date, end: date, species: str, active_ing: str) -> str:
    """Animal & Veterinary (CVM): conta eventos por data de recebimento original."""
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")
    return (
        "https://api.fda.gov/animalandveterinary/event.json"
        f"?search=animal.species:%22{species}%22"
        f"+AND+drug.active_ingredients:%22{active_ing}%22"
        f"+AND+original_receive_date:[{start_str}+TO+{end_str}]"
        "&count=original_receive_date"
    )

_task_kwargs = dict(retries=0)
if USE_POOL:
    _task_kwargs["pool"] = POOL_NAME

@task(**_task_kwargs)
def fetch_fixed_range_and_to_bq():
    url = _build_openfda_url(TEST_START, TEST_END, SPECIES, ACTIVE_ING)
    data = _openfda_get(url)
    results = data.get("results", [])
    if not results:
        return  # Sem dados no período/filtro

    # Normaliza e anota metadados do run
    df = pd.DataFrame(results).rename(columns={"count": "events"})
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)
    df["win_start"] = pd.to_datetime(TEST_START)
    df["win_end"]   = pd.to_datetime(TEST_END)
    df["species"]   = SPECIES
    df["active_ingredient"] = ACTIVE_ING

    # Grava no BigQuery
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=bq_hook.get_credentials(),
        table_schema=[
            {"name": "time", "type": "TIMESTAMP"},
            {"name": "events", "type": "INTEGER"},
            {"name": "win_start", "type": "DATE"},
            {"name": "win_end", "type": "DATE"},
            {"name": "species", "type": "STRING"},
            {"name": "active_ingredient", "type": "STRING"},
        ],
        location=BQ_LOCATION,
        progress_bar=False,
    )

@dag(
    dag_id="openfda_cvm_dog_ivermectin_range",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "cvm", "animalandveterinary", "bigquery", "range"],
)
def openfda_pipeline_cvm_range():
    fetch_fixed_range_and_to_bq()

dag = openfda_pipeline_cvm_range()
