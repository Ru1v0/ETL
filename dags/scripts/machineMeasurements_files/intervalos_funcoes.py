from datetime import datetime, timedelta, date
import logging
import pandas as pd
import requests
import json

def primeiro_ultimo_dia_mes(intervalos):
    for ano in range(2019, (date.today().year) + 1):
        for mes in range(1, 13):  # Loop através dos meses de 1 a 12
            primeiro_dia = datetime(ano, mes, 1)  # Cria o primeiro dia do mês
            ultimo_dia = primeiro_dia.replace(day = 28) + timedelta(days=4)  # Assume que o último dia é o dia 28, e ajusta para o último dia do mês
            ultimo_dia = ultimo_dia - timedelta(days =ultimo_dia.day)  # Subtrai os dias excedentes para obter o último dia correto

            today_datetime = datetime.combine(date.today(), datetime.min.time())

            if ultimo_dia <= today_datetime:
                intervalos.append((primeiro_dia.strftime('%Y-%m-%d'), ultimo_dia.strftime('%Y-%m-%d')))


def primeiro_ultimo_dia_mes_atual(intervalos):
    dia_atual = datetime.combine(date.today(), datetime.min.time())

    primeiro_dia = datetime(dia_atual.year, dia_atual.month, 1)
    ultimo_dia = datetime(dia_atual.year, dia_atual.month, dia_atual.day - 1)

    intervalos.append((primeiro_dia.strftime('%Y-%m-%d'), ultimo_dia.strftime('%Y-%m-%d')))

def ultimo_dia():
    dia_atual = datetime.combine(date.today(), datetime.min.time())
    ultimo_dia = datetime(dia_atual.year, dia_atual.month, dia_atual.day - 1)
    return ultimo_dia.strftime('%Y-%m-%d')

def extracao_imagens(thread, lista_maquinas_id, intervalos, header):
    df = pd.DataFrame()
    
    while thread < len(lista_maquinas_id): 
        maquina_id = lista_maquinas_id[thread]
        for intervalo_mensal in intervalos:    
            parametros = {"startDate" : intervalo_mensal[0], "endDate" : intervalo_mensal[1], "embed": "measurementDefinition"}
            table_url = f"https://partnerapi.deere.com/platform/machines/{maquina_id}/machineMeasurements"
            
            try:
                table_request = requests.get(url = table_url, params=parametros, headers=header)
                table_json = json.loads(table_request.text)
            except Exception as e:
                logging.error(f"Não foi possível realizar o request da máquina {maquina_id} para o url {table_url}")

            try:
                table_df = pd.json_normalize(table_json, record_path=["values"], errors="ignore")

                if table_df.empty == True:
                    table_df["startDate"] = [intervalo_mensal[0]]
                    table_df["endDate"] = [intervalo_mensal[1]]

                else:
                    table_df["startDate"] = intervalo_mensal[0]
                    table_df["endDate"] = intervalo_mensal[1]

                table_df["machine_id"] = maquina_id

                df = pd.concat([df, table_df])
            except Exception as e:
                if table_request.status_code == 403:
                    logging.error(f"Máquina {maquina_id}: sem autorização de acesso")
                elif table_request.status_code == 404:
                    logging.error(f"Máquina {maquina_id}: não existe")
                else:
                    logging.error(f"Máquina {maquina_id} com erro: {e}")
        logging.info(f"Extração da tabela da máquina {maquina_id} concluída.")
        thread += 20
    return df
