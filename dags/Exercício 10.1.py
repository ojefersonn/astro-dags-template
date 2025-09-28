from __future__ import annotations
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models import Variable
from airflow.exceptions import AirflowException
import pendulum
import pandas as pd
import requests
import logging
import time
import json
from datetime import date, datetime
from typing import Dict, List, Optional, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constantes
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos
API_TIMEOUT = 30  # segundos

# Configurações do Pipeline de Dados Farmacêuticos
# Esses valores devem ser configurados como Variáveis do Airflow no ambiente de produção
PROJECT_GCP = Variable.get("GCP_PROJECT_ID", default_var="951374833974")
DATASET_BQ = Variable.get("BIGQUERY_DATASET", default_var="dataset_medicamentos_fda")
TABELA_BQ = Variable.get("BIGQUERY_TABLE", default_var="eventos_medicamentos_periodo")
LOCALIZACAO_BQ = Variable.get("BIGQUERY_LOCATION", default_var="US")
CONEXAO_GCP_ID = Variable.get("GCP_CONNECTION_ID", default_var="google_cloud_default")
USAR_POOL = Variable.get("USE_POOL", default_var=True, deserialize_json=True)
NOME_POOL = Variable.get("POOL_NAME", default_var="api_medicamentos_pool")

# Configurações de data e consulta
DATA_INICIO_ANALISE = Variable.get("START_DATE", default_var=date(2025, 1, 1).isoformat())
DATA_FIM_ANALISE = Variable.get("END_DATE", default_var=date(2025, 12, 31).isoformat())
MEDICAMENTO_CONSULTA = Variable.get("MEDICATION_QUERY", default_var="sildenafil+citrate")

# Converter strings de data para objetos date se necessário
if isinstance(DATA_INICIO_ANALISE, str):
    DATA_INICIO_ANALISE = datetime.strptime(DATA_INICIO_ANALISE, "%Y-%m-%d").date()
if isinstance(DATA_FIM_ANALISE, str):
    DATA_FIM_ANALISE = datetime.strptime(DATA_FIM_ANALISE, "%Y-%m-%d").date()

def criar_sessao_http() -> requests.Session:
    """
    Cria e configura uma sessão HTTP com retry e headers apropriados
    
    Returns:
        requests.Session: Sessão HTTP configurada
    """
    session = requests.Session()
    
    # Configuração de retry
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # Configuração de headers
    session.headers.update({
        "User-Agent": "pipeline-medicamentos-fda/2.0 (analise.dados@institucao.gov.br)",
        "Accept": "application/json"
    })
    
    return session

# Sessão HTTP global
sessao_http = criar_sessao_http()

def obter_dados_api_fda(endpoint_url: str) -> Dict[str, Any]:
    """
    Realiza requisição à API do FDA com tratamento de erros e retry

    Args:
        endpoint_url: URL completa para consulta à API

    Returns:
        dict: Dados da resposta em formato JSON
        
    Raises:
        AirflowException: Em caso de falha na requisição após todas as tentativas
        ValueError: Se a resposta não for um JSON válido
    """
    logger.info(f"Solicitando dados da API: {endpoint_url}")
    
    try:
        resposta = sessao_http.get(endpoint_url, timeout=API_TIMEOUT)
        
        if resposta.status_code == 404:
            logger.warning("Nenhum dado encontrado para a consulta")
            return {"results": []}
            
        resposta.raise_for_status()
        
        try:
            dados = resposta.json()
            logger.info(f"Dados recebidos com sucesso. Total de itens: {len(dados.get('results', []))}")
            return dados
            
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar resposta JSON: {e}")
            raise ValueError(f"Resposta da API não é um JSON válido: {resposta.text[:200]}...")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na requisição à API: {str(e)}")
        raise AirflowException(f"Falha ao acessar a API após {MAX_RETRIES} tentativas: {str(e)}")

def construir_url_consulta_fda(inicio: date, fim: date, medicamento: str) -> str:
    """
    Constrói URL para consulta de eventos de medicamentos na API FDA
    com validação de parâmetros

    Args:
        inicio: Data de início do período
        fim: Data de fim do período
        medicamento: Nome do medicamento para consulta (será sanitizado)

    Returns:
        str: URL formatada para a API
        
    Raises:
        ValueError: Se as datas forem inválidas ou o medicamento estiver vazio
    """
    # Validação de parâmetros
    if not medicamento or not medicamento.strip():
        raise ValueError("O nome do medicamento não pode estar vazio")
        
    if not isinstance(inicio, date) or not isinstance(fim, date):
        raise ValueError("As datas devem ser objetos date")
        
    if inicio > fim:
        raise ValueError("A data de início não pode ser posterior à data de fim")
    
    # Sanitização do nome do medicamento
    medicamento_sanitizado = requests.utils.quote(medicamento.strip())
    
    # Formatação das datas
    data_inicio_fmt = inicio.strftime("%Y%m%d")
    data_fim_fmt = fim.strftime("%Y%m%d")

    # Construção da URL
    url_base = "https://api.fda.gov/drug/event.json"
    parametros_busca = f"?search=patient.drug.medicinalproduct:%22{medicamento_sanitizado}%22"
    filtro_data = f"+AND+receivedate:[{data_inicio_fmt}+TO+{data_fim_fmt}]"
    agregacao = "&count=receivedate"
    
    url_final = f"{url_base}{parametros_busca}{filtro_data}{agregacao}"
    logger.debug(f"URL da API construída: {url_final}")
    
    return url_final

# Configurações da task do Airflow
configuracoes_task = dict(retries=2, retry_delay=pendulum.duration(minutes=5))
if USAR_POOL:
    configuracoes_task["pool"] = NOME_POOL

@task(
    retries=2,
    retry_delay=pendulum.duration(minutes=5),
    pool=NOME_POOL if USAR_POOL else None,
    retry_exponential_backoff=True,
    max_retry_delay=pendulum.duration(minutes=30)
)
def processar_dados_periodo_fixo() -> None:
    """
    Extrai dados de eventos farmacêuticos da API FDA e carrega no BigQuery
    
    Esta task realiza as seguintes operações:
    1. Constrói a URL de consulta à API FDA
    2. Obtém os dados da API com tratamento de erros
    3. Processa e formata os dados
    4. Carrega os dados no BigQuery
    
    Raises:
        AirflowException: Em caso de falha na execução da task
    """
    logger.info("Iniciando processamento de dados farmacêuticos")
    logger.info(f"Período: {DATA_INICIO_ANALISE} a {DATA_FIM_ANALISE}")
    logger.info(f"Medicamento: {MEDICAMENTO_CONSULTA}")
    
    try:
        # 1. Construir URL de consulta
        url_consulta = construir_url_consulta_fda(
            DATA_INICIO_ANALISE, 
            DATA_FIM_ANALISE, 
            MEDICAMENTO_CONSULTA
        )
        
        # 2. Obter dados da API
        logger.info("Obtendo dados da API FDA...")
        dados_resposta = obter_dados_api_fda(url_consulta)
        eventos_encontrados = dados_resposta.get("results", [])

        if not eventos_encontrados:
            logger.warning("Nenhum evento encontrado no período especificado")
            return
            
        logger.info(f"Encontrados {len(eventos_encontrados)} eventos para processar")

        # 3. Processar e formatar os dados
        logger.info("Processando dados...")
        try:
            df_eventos = pd.DataFrame(eventos_encontrados)
            df_eventos = df_eventos.rename(columns={"count": "total_eventos", "time": "data_evento_str"})
            
            # Validação e limpeza dos dados
            if df_eventos.empty:
                logger.warning("Nenhum dado para processar após validação")
                return
                
            # Conversões de data e enriquecimento
            df_eventos["data_evento"] = pd.to_datetime(
                df_eventos["data_evento_str"], 
                format="%Y%m%d", 
                errors="coerce",
                utc=True
            )
            
            # Remover linhas com datas inválidas
            linhas_invalidas = df_eventos["data_evento"].isna().sum()
            if linhas_invalidas > 0:
                logger.warning(f"Removidas {linhas_invalidas} linhas com datas inválidas")
                df_eventos = df_eventos.dropna(subset=["data_evento"])
            
            # Adicionar metadados
            df_eventos["periodo_inicio"] = pd.to_datetime(DATA_INICIO_ANALISE)
            df_eventos["periodo_fim"] = pd.to_datetime(DATA_FIM_ANALISE)
            df_eventos["medicamento_analisado"] = MEDICAMENTO_CONSULTA.replace("+", " ")
            df_eventos["data_processamento"] = pd.Timestamp.now(tz="UTC")
            
            # Selecionar e ordenar colunas
            colunas_finais = [
                "data_evento", "total_eventos", "periodo_inicio", 
                "periodo_fim", "medicamento_analisado", "data_processamento"
            ]
            df_eventos = df_eventos[colunas_finais]
            
        except Exception as e:
            logger.error(f"Erro ao processar dados: {str(e)}")
            raise AirflowException(f"Falha no processamento dos dados: {str(e)}")

        # 4. Carregar dados no BigQuery
        logger.info("Conectando ao BigQuery...")
        try:
            hook_bq = BigQueryHook(
                gcp_conn_id=CONEXAO_GCP_ID, 
                location=LOCALIZACAO_BQ, 
                use_legacy_sql=False
            )

            # Schema da tabela no BigQuery
            schema_tabela = [
                {"name": "data_evento", "type": "TIMESTAMP", "mode": "REQUIRED"},
                {"name": "total_eventos", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "periodo_inicio", "type": "DATE", "mode": "REQUIRED"},
                {"name": "periodo_fim", "type": "DATE", "mode": "REQUIRED"},
                {"name": "medicamento_analisado", "type": "STRING", "mode": "REQUIRED"},
                {"name": "data_processamento", "type": "TIMESTAMP", "mode": "REQUIRED"}
            ]

            # Carregamento dos dados
            logger.info(f"Carregando {len(df_eventos)} registros no BigQuery...")
            df_eventos.to_gbq(
                destination_table=f"{DATASET_BQ}.{TABELA_BQ}",
                project_id=PROJECT_GCP,
                if_exists="append",
                credentials=hook_bq.get_credentials(),
                table_schema=schema_tabela,
                location=LOCALIZACAO_BQ,
                progress_bar=False
            )
            
            logger.info(f"Processo concluído com sucesso. {len(df_eventos)} registros carregados.")
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados no BigQuery: {str(e)}")
            raise AirflowException(f"Falha ao carregar dados no BigQuery: {str(e)}")
            
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise AirflowException(f"Falha na execução da task: {str(e)}")
    finally:
        # Garantir que a sessão HTTP seja fechada
        sessao_http.close()

@dag(
    dag_id="pipeline_analise_medicamentos_fda_v2",
    description="Pipeline para análise de eventos farmacêuticos via API FDA com tratamento de erros e monitoramento",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'data_engineering',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 2,
        'retry_delay': pendulum.duration(minutes=5),
    },
    tags=["medicamentos", "fda", "bigquery", "analise_eventos", "farmaceuticos"],
    doc_md="""
    # Pipeline de Análise de Eventos Farmacêuticos

    Este DAG realiza a extração de dados de eventos farmacêuticos da API do FDA
    e carrega os dados processados no BigQuery para análises posteriores.

    ## Funcionalidades:
    - Consulta segura à API FDA com tratamento de erros e retry
    - Validação e limpeza dos dados
    - Carregamento incremental no BigQuery
    - Monitoramento e logging detalhado

    ## Configurações:
    - Período de análise: Configurável via variável de ambiente
    - Medicamento: Configurável via variável de ambiente
    - Destino: BigQuery dataset configurável

    ## Variáveis de Ambiente Requeridas:
    - `GCP_PROJECT_ID`: ID do projeto GCP
    - `BIGQUERY_DATASET`: Nome do dataset no BigQuery
    - `BIGQUERY_TABLE`: Nome da tabela de destino
    - `GCP_CONNECTION_ID`: ID da conexão do Airflow com o GCP
    - `START_DATE`: Data de início da análise (YYYY-MM-DD)
    - `END_DATE`: Data de fim da análise (YYYY-MM-DD)
    - `MEDICATION_QUERY`: Nome do medicamento para consulta

    ## Tratamento de Erros:
    - Retry automático em falhas de rede
    - Validação de dados antes do carregamento
    - Logging detalhado para diagnóstico

    ## Monitoramento:
    - Métricas de execução
    - Alertas em caso de falha
    - Rastreamento de execuções
    """
)
def dag_pipeline_medicamentos():
    """
    DAG principal para processamento de dados farmacêuticos
    """
    processar_dados_periodo_fixo()

# Instanciação do DAG
dag_medicamentos = dag_pipeline_medicamentos()
