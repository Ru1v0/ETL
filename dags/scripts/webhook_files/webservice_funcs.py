import os 
import requests
import json
import pandas as pd
import logging
import concurrent.futures
import configs.config as main_config
from functools import partial
from configs.database import Autonomous
from datetime import datetime
from scripts.webhook_files.aux.dicts import stage_dict, event_subscription_id, remove_columns, meta_dict, record_path_dict
from configs.get_token import refresh_token_function

def verificar_registro(db_stg, stg_table_insert, fop_id, db_stg_cursor, key):
    try:
        query = f"SELECT 1 FROM {stg_table_insert} WHERE \"{key}\" = '{fop_id}'"

        cur_response = db_stg_cursor.execute(query).fetchall()

        db_stg.commit()

        return len(cur_response)

    except Exception as e:
        logging.error(f"Erro ao verificar a tabela {stg_table_insert}: {e}")
        return "Erro"

def processar_tabela_fop(evento_path, tipo_evento, API_HEADERS, stage_dict, remove_columns_dict, event_subscription_id, seq_exec_atual):
    #===============================================#

    db = Autonomous()
    db_stg = db.conect("STG")
    db_log = db.conect("LOG")
    db_stg_cursor = db_stg.cursor()
    db_log_cursor = db_log.cursor()

    #===============================================#

    evento_csv_path = "dags/scripts/webhook_files/eventos/" + tipo_evento + "/" + evento_path

    #Extração da Tabela
    event_df = pd.read_csv(evento_csv_path, index_col=False)

    chv_evento = event_subscription_id[tipo_evento]

    stg_table_insert = stage_dict[tipo_evento]

    remove_columns = remove_columns_dict[tipo_evento]

    table_urls = []
    
    try:

        for registro in range(event_df.shape[0]):
            table_urls.append(event_df["targetResource"].iloc[registro])

        for i, table_url in enumerate(table_urls):
            get_table = requests.get(table_url, headers = API_HEADERS)

            fop_id = table_url.split("/")[-1]

            if get_table.status_code == 429:
                logging.error(f"Erro no fop: {fop_id} : Erro 'Too Many Requests [429]'")
                return

            json_table = json.loads(get_table.text)
            df_table = pd.json_normalize(json_table)


            metadata_string_dados = event_df['metadata'].iloc[i].replace("'", '"')
            metadata_dados_dict = json.loads(metadata_string_dados)
            df_metadata = pd.DataFrame(metadata_dados_dict)

            df_metadata_tipo = df_metadata[df_metadata["key"] == "fieldOperationType"]
            fop_type_id = df_metadata_tipo.iloc[0, 1]

            if get_table.status_code == 404:
                tpo_acao = "Deletado"
                db.clear(db_stg, stg_table_insert, where = f"\"id\" = '{fop_id}'")

            else:
                #Transformações necessárias
                df_metadata_field = df_metadata[df_metadata["key"] == "fieldId"]
                field_id = df_metadata_field.iloc[0, 1]
                df_table["field_id"] = field_id

                if tipo_evento in ["fop_har", "fop_see"]:
                    for coluna in df_table.columns:
                        if coluna in ["@type", "links", "cropName"]:
                            df_table = df_table.drop(columns=[coluna])

                    varieties_string_dados = df_table['varieties'].astype(str).iloc[0].replace("'", '"').replace("False", "\"False\"").replace("True", "\"True\"")
                    varieties_dados_dict = json.loads(varieties_string_dados)
                    df_varieties = pd.DataFrame(varieties_dados_dict)

                    df_table = pd.concat([df_table]*len(df_varieties)).reset_index(drop=True)
                    df_table = pd.concat([df_table, df_varieties], axis = 1)

                for coluna in remove_columns:
                    if coluna in df_table.columns:            #se a coluna pertence à lista de colunas do dataframe principal
                        df_table = df_table.drop(columns=[coluna])  #remove a coluna do dataframe principal

                #Carregar a Tabela na Stage
                verificação_registro = verificar_registro(db_stg, stg_table_insert, fop_id, db_stg_cursor, key = "id")

                if verificação_registro > 0: 
                    tpo_acao = "Modificado"
                    try:
                        db.clear(db_stg, stg_table_insert, where = f"\"id\" = '{fop_id}'")
                        db.insert(db_stg, db_stg_cursor, stg_table_insert, df_table.astype(str))
                    except Exception as e:
                        logging.error(f"Modificação do registro {fop_id} da tabela {stg_table_insert} com erro: {e}")
                        print(df_table)
                        tpo_acao = "Erro"

                else:
                    tpo_acao = "Criado"
                    try:
                        db.insert(db_stg, db_stg_cursor, stg_table_insert, df_table.astype(str))
                    except Exception as e:
                        logging.error(f"Criação do registro {fop_id} da tabela {stg_table_insert} com erro: {e}")
                        print(df_table)
                        tpo_acao = "Erro"



            #Carregar Log Da Tabela
            nom_tabela = "fieldOperation"
            tpo_tabela = fop_type_id 
            chv_tabela = fop_id 
            dta_hora =  datetime.now()

            if tpo_acao != "Erro":

                log_df = pd.DataFrame({
                    "NOM_TABELA"    : [nom_tabela],
                    "CHV_EVENTO"    : [chv_evento],
                    "TPO_TABELA"    : [tpo_tabela],
                    "CHV_TABELA"    : [chv_tabela],
                    "DTA_HORA"      : [dta_hora],
                    "TPO_ACAO"      : [tpo_acao],
                    "SEQ_EXECUCAO"  : [seq_exec_atual]
                })

                logging.info(f"Tabela {nom_tabela + tpo_tabela} de id = {chv_tabela}, {tpo_acao} em {stg_table_insert} às {dta_hora}, Sequência de Execução = {seq_exec_atual}")

                db.insert(db_log, db_log_cursor, "LOG_WEBHOOK", log_df.astype(str))
    
    except Exception as e:
        logging.error(f"Tabela fieldOperation de id = {fop_id} com Erro: {e}")

    os.remove(evento_csv_path)
    db_stg_cursor.close()
    db_log_cursor.close()

def processar_tabela_measurementType(row, meta_dict, API_HEADERS, record_path_dict, remove_columns_dict, seq_exec_atual):
    #===============================================#

    db = Autonomous()
    db_stg = db.conect("STG")
    db_log = db.conect("LOG")
    db_stg_cursor = db_stg.cursor()
    db_log_cursor = db_log.cursor()

    #===============================================#

    #Obtenção de dados
    fop_id = row[0]
    tpo_tabela = row[1]
    tpo_acao = None #valor padrão
    tabela_dict = "measurementTypes_" + tpo_tabela 
    stg_table_insert = stage_dict[tabela_dict]
    remove_columns = remove_columns_dict[tabela_dict]
    url_request = f"https://partnerapi.deere.com/platform/fieldOperations/{fop_id}/measurementTypes"
    meta = meta_dict[tabela_dict]
    recordPath = record_path_dict[tabela_dict]

    try:

        #Request na url
        request = requests.get(url_request, headers=API_HEADERS)

        if request.status_code == 429:
            logging.error(f"Erro no fop: {fop_id} : Erro 'Too Many Requests [429]'")
            return

        request_json = json.loads(request.text)

        if request.status_code == 404:
            tpo_acao = "Deletado"
            db.clear(db_stg, stg_table_insert, where = f"\"fop_id\" = '{fop_id}'")

        #Tratamento de Dados    
        elif len(request_json['values']) > 0:
            if len(meta) > 0:
                table_df = pd.json_normalize(request_json['values'], record_path = recordPath, meta = meta, errors = "ignore")

            else:
                table_df = pd.json_normalize(request_json, record_path = recordPath, errors = "ignore")

            table_df["fop_id"] = fop_id

            for coluna in remove_columns:
                if coluna in table_df.columns:
                    table_df = table_df.drop(columns=[coluna])

            #Carregar Tabela
            verificação_registro = verificar_registro(db_stg, stg_table_insert, fop_id, db_stg_cursor, key = "fop_id")

            if verificação_registro > 0: 
                tpo_acao = "Modificado"
                try:
                    db.clear(db_stg, stg_table_insert, where = f"\"fop_id\" = '{fop_id}'")
                    db.insert(db_stg, db_stg_cursor, stg_table_insert, table_df.astype(str))
                except Exception as e:
                    logging.error(f"Modificação do registro {fop_id} da tabela {stg_table_insert} com erro: {e}")
                    tpo_acao = "Erro"

            else:
                tpo_acao = "Criado"
                try:
                    db.insert(db_stg, db_stg_cursor, stg_table_insert, table_df.astype(str))
                except Exception as e:
                    logging.error(f"Inserção do registro {fop_id} da tabela {stg_table_insert} com erro: {e}")
                    tpo_acao = "Erro"
        else:
            tpo_acao = "Erro"

        #Carregar Log
        if tpo_acao != "Erro":

            dta_hora =  datetime.now()

            log_df = pd.DataFrame({
                "NOM_TABELA"    : "MeasurementType",
                "CHV_EVENTO"    : "Incremental",
                "TPO_TABELA"    : [tpo_tabela],
                "CHV_TABELA"    : ["Originário de fop: " + fop_id],
                "DTA_HORA"      : [dta_hora],
                "TPO_ACAO"      : [tpo_acao],
                "SEQ_EXECUCAO"  : [seq_exec_atual]
            })

            logging.info(f"Tabela MeasurementType{tpo_tabela} de fop_id = {fop_id}, {tpo_acao} em {stg_table_insert} às {dta_hora}, Sequência de Execução = {seq_exec_atual}")

            db.insert(db_log, db_log_cursor, "LOG_WEBHOOK", log_df.astype(str))
        
    except Exception as e:
        logging.error(f"Tabela MeasurementType{tpo_tabela} de fop_id = {fop_id} com erro: {e}")

    db_stg_cursor.close()
    db_log_cursor.close()
    