from configs.database import Autonomous
from datetime import datetime
import logging


def log_processamento(tipo_execucao, estado_execucao, numero_registros):
    db = Autonomous()
    db_log = db.conect("LOG")
    tipo_de_execucoes = {"carga_full" : "da carga da STAGE.API", "webhook" : "do processamento do Webhook", "machineMeasurements": "da carga das Imagens da Machine Measurements"}

    hora_final = ""
    total_registros = -1
    status = "I"
    observacao = f"Início {tipo_de_execucoes[tipo_execucao]}...."

    if estado_execucao == "final":
        hora_final = datetime.now()
        total_registros = numero_registros
        status = "F"
        observacao = f".. fim {tipo_de_execucoes[tipo_execucao]}"

    try:
        db_log_cur = db_log.cursor()
        nome_banco = 'AUTONOMOUS'
        nome_objeto = 'STG.API'
        hora_inicial = datetime.now()

        fn_log_proce = 'FN_INSERT_LOG_PROCESSAMENTO'

        log_proce_args = [
            nome_banco,
            nome_objeto,
            hora_inicial,
            hora_final,
            total_registros,
            observacao,
            status]
        
        db_log_cur.callfunc(fn_log_proce, str, log_proce_args)

        logging.info("Função Log Processamento Inicial Executada")

    except Exception as e:
        logging.error(f"Função Log Processamento Inicial com Erro: {e}")
    else:
        db_log_cur.close()