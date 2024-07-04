from dags.configs.log_processamento import log_processamento
from dags.configs.get_token import refresh_token_function
from scripts.cargaFull_files.table import Table
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from configs.database import Autonomous
from airflow.models import Variable
from configs import config as cfg
from datetime import timedelta
import concurrent.futures
import pandas as pd
import logging
import time

# Configurações padrão da DAG
default_args = {
    'owner': 'Null',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}

@dag('ETL_Iguacu_Tabelas_Diárias', schedule_interval=None, default_args=default_args, catchup=False, tags=['API', 'Centro de Operações', 'Diário'])
def ETL_Tabelas_Diario():
    def executar_tabela(tabela, access_token):
        db = Autonomous()
        db_stg = db.conect("STG")
        db_log = db.conect("LOG")

        registros_totais = 0

        try:
            logging.info(f"ETL da Tabela {tabela}")
            objeto = Table(tabela, db, db_stg, db_log, access_token)

            logging.info("Extração...")
            if tabela != "organizations":
                with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                    lista_dataframes = list(
                        executor.map(objeto.extract, range(20)))
                for dataframe in lista_dataframes:
                    objeto.df = pd.concat([objeto.df, dataframe])
            else:
                objeto.df = objeto.extract(0)

            logging.info("Verificação...")
            objeto.verify()

            logging.info("Carregamento...")
            objeto.load()

            registros_totais += objeto.linhas_df()

            logging.info(f"ETL da Tabela {tabela} Concluído")

        except Exception as e:
            logging.error(f"Erro no ETL da tabela {tabela}: {e}")

        if tabela == "measurementTypesSeeding":
            access_token = refresh_token_function(cfg.REFRESH_TOKEN)
            time.sleep(20)
            logging.info(f"Token Atualizado: {access_token}")
        logging.info("======================================================================================")
        return registros_totais

    @task(task_id="Log_Processamento_Inicial")
    def log_processamento_inicial():
        # Simulando a função log_processamento
        log_processamento("carga_full", "inicial", 0)
        logging.info("Processamento inicial completo")
        return 0
    
    @task(task_id = "token_de_acesso")
    def token_de_acesso():
        token_de_acesso = refresh_token_function(cfg.REFRESH_TOKEN)

        logging.info(f"Token de Acesso: {token_de_acesso}")
        logging.info(f"Token de Atualização: {cfg.REFRESH_TOKEN}")
        return token_de_acesso

    @task(task_id="extracao_organizations")
    def extracao_organizations(total_regs, access_token):
        tabelas = ["organizations"]

        for tabela in tabelas:
            total_regs += executar_tabela(tabela, access_token)
        return total_regs

    @task(task_id="extracao_nivel_1")
    def extracao_nivel_1(total_regs, access_token):
        tabelas = ["chemicals", "clients", "fertilizers", "machines", "operators", "varieties",]
        for tabela in tabelas:
            total_regs += executar_tabela(tabela, access_token)
        return total_regs
        

    @task(task_id="extracao_nivel_2")
    def extracao_nivel_2(total_regs, access_token):
        tabelas = ["alerts", "farms", "machinesMeasurements", "machinesMeasurementsIntervals", "machinesMeasurementsBuckets",]

        for tabela in tabelas:
            total_regs += executar_tabela(tabela, access_token)
        return total_regs

    @task(task_id="extracao_fields")
    def extracao_fields(total_regs, access_token):
        tabelas = ["fields"]

        for tabela in tabelas:
            total_regs += executar_tabela(tabela, access_token)
        return total_regs

    @task(task_id="Log_Processamento_Final")
    def log_processamento_final(total_regs):
        log_processamento("carga_full", "final", total_regs)
        logging.info(f"Processamento final completo: {total_regs} registros processados")

    total_regs = log_processamento_inicial()
    access_token = token_de_acesso()
    total_regs = extracao_organizations(total_regs, access_token)
    total_regs = extracao_nivel_1(total_regs, access_token)
    total_regs = extracao_nivel_2(total_regs, access_token)
    total_regs = extracao_fields(total_regs, access_token)
    log_processamento_final(total_regs)

dag = ETL_Tabelas_Diario()