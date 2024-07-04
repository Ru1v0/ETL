from dags.configs.get_token import refresh_token_function
from scripts.cargaFull_files.table import Table
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable
from configs import config as cfg
from datetime import timedelta
from scripts.webhook_files.update_events import formatar_json, verificar_orgs, terminar_inscricoes
import pandas as pd
import logging
import requests
import concurrent.futures
from configs.database import Autonomous
from functools import partial
import json
from scripts.webhook_files.aux.dicts import formatacao_dict

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

@dag('Atualizar_Eventos', schedule_interval=None, default_args=default_args, catchup=False, tags=['API', 'Webhook', 'Eventos'])
def atualizar_eventos():
    @task(task_id = "desativar_eventos")
    def desativar_eventos():
        terminar_inscricoes()
    
    @task(task_id = "atualizar_eventos")
    def atualizar():
        db = Autonomous()
        db_stg_cursor = db.conect("STG").cursor()

        query = "SELECT DISTINCT \"org_id\" FROM STAGE.VW_RAW_DATA_FIELDS"

        lista_orgs = db_stg_cursor.execute(query).fetchall()

        lista_org_ids = [org for org in lista_orgs if org != None]

        headers_get = {'Authorization': f'Bearer {refresh_token_function(cfg.REFRESH_TOKEN)}', 'Accept': 'application/vnd.deere.axiom.v3+json', 'Content-Type': 'application/vnd.deere.axiom.v3+json'}
        verificar_orgs_parcial = partial(verificar_orgs, headers_request = headers_get)

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            lista_orgs = executor.map(verificar_orgs_parcial, lista_org_ids)

        orgs_com_acesso = [org for org in lista_orgs]

        #==========================================================================#

        data_str = """
        {
          "eventTypeId": {},
          "filters": [
            {
              "key": "orgId",
              "values": {}
            },
            {
                "key": "fieldOperationType",
                "values": {}
            }
          ],
          "targetEndpoint": {
            "targetType": "https",
            "uri": {}
          },
          "status": {},
          "displayName": {}
        }
        """

        data_json = json.loads(data_str)
        hooks = ["applications", "harvest", "seeding", "tillage"]
        headers_post = {"Content-Type": "application/vnd.deere.axiom.v3+json", "Accept": "application/vnd.deere.axiom.v3+json", "Authorization": f"{refresh_token_function(cfg.REFRESH_TOKEN)}"}

        for hook in hooks:
            json_final = formatar_json(data_json, formatacao_dict[hook], orgs_com_acesso)
            request = requests.post(url = "https://partnerapi.deere.com/platform/eventSubscriptions", headers=headers_post, json=json_final)

            if request.status_code in [200, 201]:
                logging.info(f"A criação do evento {hook} foi bem sucedida!")
            else:
                logging.critical(f"A criação do evento NÃO foi bem sucedida. Código da Resposta: {request.status_code}")


    desativar_eventos() >> atualizar()
    
        

dag = atualizar_eventos()