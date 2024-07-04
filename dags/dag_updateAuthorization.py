from dags.configs.get_token import refresh_token_function
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable
from configs import config as cfg
from datetime import timedelta
import logging
import random
import rsa
import requests
import string



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

@dag('Atualizar_Autorizacaoo', schedule_interval=None, default_args=default_args, catchup=False, tags=['API', 'Webhook', 'Autorização'])
def atualizar_autorizacao():
    @task(task_id = "atualizar_autorizacaoo")
    def atualizar():
        # Geração de chave pública e privada, bem como a mensagem para ser assinada
        public_key, private_key = rsa.newkeys(2048)
        encoded_key = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(232))

        #Registra Chave Privada em um arquivo .PEM 
        with open("webserver/cert/private.pem", "wb") as file:
            file.write(private_key.save_pkcs1("PEM"))

        with open("webserver/cert/public.pem", "wb") as file:
            file.write(public_key.save_pkcs1("PEM"))

        #Registra chave assinada num arquivo txt
        with open("webserver/cert/encoded_key", "wb") as file:
            file.write(encoded_key.encode("utf8"))

        #Cria assinatura da chave "aleatória" com a chave privada e registra em um arquivo binário
        signature = rsa.sign(encoded_key.encode("utf8"), private_key, "SHA-256")
        with open("webserver/cert/signature", "wb") as file:
            file.write(signature)


        with open("webserver/cert/public.pem", "rb") as file:
            public_key_bytes = file.read()

        public_key_PEM = public_key_bytes.decode("utf8")

        update_json = {
          "authorizationHeaderValue": f"{public_key_PEM}",
          "concurrentDeliveries": 5,
          "maxBatchSize": 5,
          "status": "Active"
        }

        update_json["authorizationHeaderValue"] = update_json["authorizationHeaderValue"].replace("\n", cfg.AUTH_TOKEN_ALIAS)

        header = {'Authorization': f'Bearer {refresh_token_function(cfg.REFRESH_TOKEN)}', 'Accept': 'application/vnd.deere.axiom.v3+json', 'Content-Type': 'application/vnd.deere.axiom.v3+json'}

        response = requests.patch("https://partnerapi.deere.com/platform/eventSubscriptionDelivery", headers=header, json=update_json)

        if response.status_code == 204:
            logging.info("Chave de autorização atualizada com sucesso!")
        else:
            logging.info("NÃO foi possível atualizar a chave de autorização")

    atualizar()

dag = atualizar_autorizacao()