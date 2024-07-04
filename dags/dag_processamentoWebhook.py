from dags.scripts.webhook_files.aux.dicts import event_subscription_id, stage_dict, remove_columns, meta_dict, record_path_dict
from scripts.webhook_files.webservice_funcs import processar_tabela_fop, processar_tabela_measurementType
from dags.configs.log_processamento import log_processamento
from dags.configs.get_token import refresh_token_function
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from configs.database import Autonomous
from airflow.models import Variable
from configs import config as cfg
from datetime import timedelta
from functools import partial
from datetime import datetime
import concurrent.futures
import logging
import time
import os

# Configurações padrão da DAG
default_args = {
    'owner': 'Null',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}

@dag('Processamento_Webhook', schedule_interval="20 21 * * *", default_args=default_args, catchup=False, tags=['Webhook', 'API', 'Diário'])
def Processamento_Webhook():
    @task(task_id = "Log_Processamento_Inicial")
    def log_processamento_inicial():
        log_processamento("webhook", "inicial", 0)
        logging.info("Processamento inicial completo")
    
    @task(task_id = "Processamento_fieldOperations")
    def processamento_fieldOperations(**kwargs):
        db = Autonomous()
        db_log = db.conect("LOG")
        db_log_cursor = db_log.cursor()
        
        query = "SELECT MAX(DISTINCT TO_NUMBER(\"SEQ_EXECUCAO\")) FROM CONTROLE.LOG_WEBHOOK ORDER BY \"SEQ_EXECUCAO\" DESC"
        cur_response = db_log_cursor.execute(query).fetchall()
        if cur_response[0][0] == None:
            seq_exec_atual = 0
        else:
            seq_exec_atual = int(cur_response[0][0]) + 1 

        P_ACCESS_TOKEN = refresh_token_function(cfg.REFRESH_TOKEN)
        API_HEADERS = {'Authorization': f'Bearer {P_ACCESS_TOKEN}', 'Accept': 'application/vnd.deere.axiom.v3+json', 'No_Paging': 'True'}
        time.sleep(5)

        tipos_eventos = [
            "fop_app",
            "fop_har",
            "fop_til",
            "fop_see"
        ]

        for evento in tipos_eventos:
            evento_dir = os.listdir("dags/scripts/webhook_files/eventos/" + evento) #Lista de caminhos do diretório
            processar_tabela_fop_com_parametros = partial(processar_tabela_fop, tipo_evento = evento, API_HEADERS = API_HEADERS, stage_dict = stage_dict, remove_columns_dict = remove_columns, 
                                                          event_subscription_id = event_subscription_id, seq_exec_atual = seq_exec_atual)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                resultados = executor.map(processar_tabela_fop_com_parametros, evento_dir)

            for resultado in resultados:
                pass
        
        db_log_cursor.close()
        
        # Passa a variável seq_exec_atual para as próximas tasks
        kwargs['ti'].xcom_push(key='seq_exec_atual', value=seq_exec_atual)
    
    @task(task_id = "Processamento_measurementTypes")
    def processamento_measurementTypes(**kwargs):
        # Recupera a variável seq_exec_atual do XCom
        seq_exec_atual = kwargs['ti'].xcom_pull(key='seq_exec_atual', task_ids='Processamento_fieldOperations')
        
        db = Autonomous()
        db_log = db.conect("LOG")
        db_log_cursor = db_log.cursor()

        P_ACCESS_TOKEN = refresh_token_function(cfg.REFRESH_TOKEN)
        API_HEADERS = {'Authorization': f'Bearer {P_ACCESS_TOKEN}', 'Accept': 'application/vnd.deere.axiom.v3+json', 'No_Paging': 'True'}
        time.sleep(5)

        data_hoje = datetime.now().strftime("%Y%m%d")
        query = f"SELECT CHV_TABELA, TPO_TABELA FROM LOG_WEBHOOK WHERE NOM_TABELA = 'fieldOperation' AND SEQ_EXECUCAO = {seq_exec_atual}"

        lista_registros_query = db_log_cursor.execute(query).fetchall()
        db_log.commit()

        processar_tabela_measurementType_com_parametros = partial(processar_tabela_measurementType, meta_dict = meta_dict, API_HEADERS = API_HEADERS, record_path_dict = record_path_dict, 
                                                                  remove_columns_dict = remove_columns, seq_exec_atual = seq_exec_atual)

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            resultados = executor.map(processar_tabela_measurementType_com_parametros, lista_registros_query)

            for resultado in resultados:
                pass
        
        db_log_cursor.close()
    
    @task(task_id = "Log_Processamento_Final")
    def log_processamento_final(**kwargs):
        # Recupera a variável seq_exec_atual do XCom
        seq_exec_atual = kwargs['ti'].xcom_pull(key='seq_exec_atual', task_ids='Processamento_fieldOperations')
        
        db_log = Autonomous().conect("LOG")
        db_log_cur = db_log.cursor()

        query = f"SELECT COUNT(*) FROM CONTROLE.LOG_WEBHOOK WHERE \"SEQ_EXECUCAO\" = {seq_exec_atual}"
        response = db_log_cur.execute(query)
        response_list = response.fetchall()
        total_regs = response_list[0][0]

        db_log_cur.close()

        log_processamento("webhook", "final", total_regs)
        logging.info("Processamento Final Completo")

    log_processamento_inicial() >> processamento_fieldOperations() >> processamento_measurementTypes() >> log_processamento_final()

dag = Processamento_Webhook()