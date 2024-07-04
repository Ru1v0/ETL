from scripts.machineMeasurements_files.intervalos_funcoes import primeiro_ultimo_dia_mes, primeiro_ultimo_dia_mes_atual, extracao_imagens
from dags.configs.log_processamento import log_processamento
from dags.configs.get_token import refresh_token_function
from scripts.cargaFull_files.table import Table
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from configs.database import Autonomous
from airflow.models import Variable
from oracledb import  DatabaseError
from configs import config as cfg
from datetime import timedelta
from datetime import datetime
from functools import partial
import concurrent.futures
import pandas as pd
import logging

# Configurações padrão da DAG
default_args = {
    'owner': 'Iguaçu Máquinas',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}

@dag('Processo_machineMeasurement', schedule_interval=None, default_args=default_args, catchup=False, tags=['API', 'Centro de Operações', 'Imagens', 'Diário'])
def Processo_machineMeasurement():
    @task(task_id = "Log_Processamento_Inicial")
    def log_processamento_inicial():
        log_processamento("machineMeasurements", "inicial", 0)
        logging.info("Processamento inicial completo")
        return 0

    @task(task_id = "ETL_Imagens")
    def ETL_imagens(total_regs):
        intervalos = []

        """
        if datetime.today().day == 1:
            TPO_EXEC = "FULL"
            primeiro_ultimo_dia_mes(intervalos)
        else:
            TPO_EXEC = "DIA"
            primeiro_ultimo_dia_mes_atual(intervalos)
        """
        primeiro_ultimo_dia_mes(intervalos)

        logging.info("Intervalos gerados.")

        df_maquinas_total = pd.DataFrame()

        authorization_token = refresh_token_function(cfg.REFRESH_TOKEN)
        header = {'Authorization': f'Bearer {authorization_token}', 'Accept': 'application/vnd.deere.axiom.v3+json', 'No_Paging': 'True'}

        db = Autonomous()
        db_stg = db.conect("STG")
        db_log = db.conect("LOG")
    
        colunas, valores = db.select(db_stg, "VW_RAW_DATA_MACHINES")   #seleciona colunas e valores da tabela na STAGE
        select_dataframe = pd.DataFrame(valores, columns = colunas) #pega os valores e colunas e os transforma em um dataframe
        lista_maquinas_id = list(select_dataframe["id"])

        logging.info("Início da extração das tabelas")

        extracao_imagens_partial = partial(extracao_imagens, lista_maquinas_id = lista_maquinas_id, intervalos = intervalos, header = header)

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            lista_df_maquinas = list(executor.map(extracao_imagens_partial, range(20)))

        for df_maquina in lista_df_maquinas:
            df_maquinas_total = pd.concat([df_maquinas_total, df_maquina])

        df_maquinas_total = df_maquinas_total.drop(columns=["links"])

        logging.info("Extração das machine measurements concluídas")

        P_HORA_FINAL = datetime.now()

        #======================================================================================================================#
        #INSERIR DATAFRAME NA BASE AUTONOMOUS:
        try:
            db_stg_cursor = db_stg.cursor()    #abre um cursor para stage
            if df_maquinas_total.shape[0] > 0:    #caso o número de registros do dataframe seja maior que 0
                if TPO_EXEC == "FULL":
                    db.clear(db_stg, "STAGE.RAW_DATA_HISTORICO_MEASUREMENT_MACHINES")  #limpa a tabela na stage
                else:
                    db.clear(db_stg, "STAGE.RAW_DATA_HISTORICO_MEASUREMENT_MACHINES", where = f"\"startDate\" = '{intervalos[0][0]}' AND \"endDate\" = '{intervalos[0][1]}'")
                db.insert(db_stg, db_stg_cursor, "STAGE.RAW_DATA_HISTORICO_MEASUREMENT_MACHINES", df_maquinas_total.astype(str)) #insere a tabela na stage
                total_regs += df_maquinas_total.shape[0]

            P_STATUS = "S" #define o status da carga como sucesso

            logging.info(f"Historico Measurement Machines inserida com sucesso no Autonomous com destino à tabela STAGE.RAW_DATA_HISTORICO_MEASUREMENT_MACHINES, contendo {df_maquinas_total.shape[0]} registros")

        except DatabaseError:   #caso haja erro de inserção na stage (tamanho da tabela muito grande)
            batch_size = 50000    #a execução é separada em lotes de 50000 registros cada

            def separador(df_maquinas_total, batch_size):
                return(df_maquinas_total[pos:pos+batch_size] for pos in range(0,len(df_maquinas_total),batch_size))   #retorna uma lista com dataframes já selecionados

            db_stg_cursor = db_stg.cursor()    #abre cursor para stage

            for batch in separador(df_maquinas_total, batch_size):  #para cada lote retornado na função
                db.insert(db_stg, db_stg_cursor, "STAGE.RAW_DATA_HISTORICO_MEASUREMENT_MACHINES", batch.astype(str)) #insere o lote na stage
                total_regs += batch.shape[0]

            db_stg_cursor.close()   #fecha cursor da stage

            P_STATUS = "S"     #define o status da carga como sucesso

            logging.info(f"Tabela Historico Measurement Machines inserida com sucesso no Autonomous com destino à tabela STAGE.RAW_DATA_HISTORICO_MEASUREMENT_MACHINES, contendo {df_maquinas_total.shape[0]} registros")

        except Exception as e:
            P_STATUS = "E" #define o status da carga com oerro

            logging.error(e)

        else:   
            db_stg_cursor.close() #fecha o cursor da stage
        
        return total_regs

    @task(task_id = "Log_Processamento_Final")
    def log_processamento_final(total_regs):
        log_processamento("machineMeasurements", "final", total_regs)
        logging.info("Processamento final completo")
    
    total_regs = log_processamento_inicial()
    total_regs = ETL_imagens(total_regs)
    log_processamento_final(total_regs)

dag = Processo_machineMeasurement()