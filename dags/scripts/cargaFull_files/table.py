import json
import logging
import requests
import pandas as pd
from datetime import datetime
from oracledb import DatabaseError
from dags.scripts.cargaFull_files.dicts import url_tabelas, select_tables, url_valores, recordPath_dict, params_dict ,meta_dict, include_column_dict, remove_column_dict, stage_include_dict
from configs.database import Autonomous


class Table():
    def __init__(self, tabela_nome, db, db_stg, db_log, token):
        self.df          = pd.DataFrame()
        self.tabela_nome = tabela_nome

        #API
        self.header = {'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.deere.axiom.v3+json', 'No_Paging': 'True'}

        #DATABASE
        self.db     = db
        self.db_stg = db_stg
        self.db_log = db_log

        #CARGA
        self.P_NOME_TABELA     = tabela_nome
        self.P_HORA_INICIAL    = datetime.now()
        self.P_HORA_FINAL      = None
        self.P_TOTAL_REGS      = 0
        self.P_STATUS          = None
        self.P_PERIODO_INICIAL = self.P_HORA_INICIAL.strftime("%-d-%b-%-y")
        self.P_PERIODO_FINAL   = None

        #Dicionários
        self.url_extracao   = url_tabelas[tabela_nome]          # Tabelas para acessar links;
        self.url_valores    = url_valores[tabela_nome]          # Valores necessários para montar url;
        self.select_table   = select_tables[tabela_nome]        # Qual tabela será selecionada na STAGE;
        self.recordPath     = recordPath_dict[tabela_nome]      # Path para salvar a tabela; 
        self.parametros     = params_dict[tabela_nome]          # Parâmetros da tabela;
        self.meta           = meta_dict[tabela_nome]            # Metadados da tabela;
        self.remove_column  = remove_column_dict[tabela_nome]   # Campos a serem excluídos;
        self.include_column = include_column_dict[tabela_nome]  # Campos a serem inseridos;
        self.stage_include  = stage_include_dict[tabela_nome]   # Nome da tabela no Autonomous(Stage);

        self.get_number = 0

    def extract(self, thread:int):      
        try:
            #Configurações Iniciais do Método de Extração
            df = pd.DataFrame() #DataFrame que armazenará todos os registros de todas as organizações da tabela (Ex: Machine, fields, etc)
            thread_number = thread

            if self.tabela_nome == "organizations":
                org_request        = requests.get('https://partnerapi.deere.com/platform/organizations', headers=self.header)
                self.get_number += 1
                org_json_data      = json.loads(org_request.text)               # Obtém o body da request;
                org_result         = pd.json_normalize(org_json_data, record_path = ['values'], errors='ignore')         # Transformando o JSON em Dataframe;

                for i in range(org_result.shape[0]):    #para cada organização no dataframe das organizações
                    org_link = pd.json_normalize(org_result.iloc[i, 6]) #obtém apenas os links da organização
                    mascara = org_link['rel'].isin(['manage_connection'])   #mascara para identificar se há acesso para aquela organização ou não

                    if org_link[mascara].empty == False:    #caso haja acesso
                        organization_registro_series = org_result.iloc[i]   #obtém o registro inteiro daquela organização (em forma de pandas.series)
                        organization_registro_df = organization_registro_series.to_frame().T    #transforma a series para um dataframe
                        df = pd.concat([df, organization_registro_df], axis=0, ignore_index=True) #concatena o registro para o dataframe principal

            else:
                nome_df_final = self.include_column[0]  #nome da coluna na tabela final
                nome_select = self.include_column[1]    #nome da coluna onde haverá o select

                colunas, valores = self.db.select(self.db_stg, self.select_table)   #seleciona colunas e valores da tabela na STAGE
                select_dataframe = pd.DataFrame(valores, columns = colunas) #pega os valores e colunas e os transforma em um dataframe

                lista_dataframes = []
                for valor in self.url_valores:  #lista com coluna(s) dos valores para compor a url da tabela
                    lista_dataframes.append(list(select_dataframe[valor]))  #seleciona valor necessário para compor a url da tabela

                while thread < select_dataframe.shape[0]:   #enquanto a iésima iteração for menor que a quantidade de registros da tabela selecionada
                    try:
                        url_request = self.url_extracao.format(*lista_url_func(lista_dataframes, thread, len(self.url_valores))) #faz a formatação
                        # da string (url no dicionário de dados) necessária para obter os valores da tabela

                        request = requests.get(url_request, params=self.parametros, headers=self.header) #obtém o request em cima da url da tabela
                        self.get_number += 1
                        json_request = json.loads(request.text) #transforma o body do request em um json

                        if len(self.meta) > 0:  #caso seja necessário usar meta, a transformação de json para dataframe é diferente
                            result = pd.json_normalize(json_request['values'], record_path=self.recordPath, meta=self.meta, errors="ignore")
                        else:
                            result = pd.json_normalize(json_request, record_path=self.recordPath, errors="ignore")
                        
                        id = list(select_dataframe[nome_select])    #variável id recebe o valor de acordo com seu nome no dicionário
                        result[nome_df_final] = str(id[thread])     #criada coluna chave para joins no Autonomous, com valor = id


                        df = pd.concat([df, result], ignore_index=True) #concatena as tabelas no dataframe principal seguindo get por get

                    except Exception as w:
                        logging.warning(f"Extração tabela {self.tabela_nome}: {w}")

                    thread += 20

            for coluna in self.remove_column:          #para cada coluna que necessita-se remover
                if coluna in df.columns:            #se a coluna pertence à lista de colunas do dataframe principal
                    df = df.drop(columns=[coluna])  #remove a coluna do dataframe principal

            self.P_HORA_FINAL = datetime.now()     
        
        except Exception as e:
            logging.error(f"Erro {self.tabela_nome}: {e}")

        logging.info(f"Dataframe da tabela {self.tabela_nome} gerada com {df.shape[0]} registros pelo {thread_number}°")

        return df
        

    def load(self):

        #INSERIR DATAFRAME NA BASE AUTONOMOUS:
        try:
            db_stg_cursor = self.db_stg.cursor()    #abre um cursor para stage
            if self.df.shape[0] > 0:    #caso o número de registros do dataframe seja maior que 0
                self.db.clear(self.db_stg, self.stage_include)  #limpa a tabela na stage
                self.db.insert(self.db_stg, db_stg_cursor, self.stage_include, self.df.astype(str)) #insere a tabela na stage
            self.P_STATUS = "S" #define o status da carga como sucesso
            logging.info(f"Tabela {self.tabela_nome} inserida com sucesso no Autonomous com destino à tabela {self.stage_include}, contendo {self.df.shape[0]} registros")

        except DatabaseError:   #caso haja erro de inserção na stage (tamanho da tabela muito grande)
            batch_size = 50000    #a execução é separada em lotes de 50000 registros cada

            def separador(df, batch_size):
                return(df[pos:pos+batch_size] for pos in range(0,len(df),batch_size))   #retorna uma lista com dataframes já selecionados
            
            db_stg_cursor = self.db_stg.cursor()    #abre cursor para stage

            for batch in separador(self.df, batch_size):  #para cada lote retornado na função
                self.db.insert(self.db_stg, db_stg_cursor, self.stage_include, batch.astype(str)) #insere o lote na stage
            
            db_stg_cursor.close()   #fecha cursor da stage
            self.P_STATUS = "S"     #define o status da carga como sucesso

            logging.info(f"Tabela {self.tabela_nome} inserida com sucesso no Autonomous com destino à tabela {self.stage_include}, contendo {self.df.shape[0]} registros")

        except Exception as e:
            self.P_STATUS = "E" #define o status da carga com oerro
            logging.error(e)
        else:   
            db_stg_cursor.close() #fecha o cursor da stage
        
        logging.info(f"Tabela {self.tabela_nome} com {self.get_number} requests.")

        #INSERIR CARGA LOG NA BASE AUTONOMOUS
        self.P_TOTAL_REGS = self.df.shape[0]    #obtém o número de registros do dataframe
        self.P_PERIODO_FINAL = self.P_HORA_FINAL.strftime("%-d-%b-%-y") #define o período final de carga daquela tabela

        try:
            db_log_cursor = self.db_log.cursor()    #abre um cursor para o CONTROLE
            log_carga_args = [
                "AUTONOMOUS", #Nome banco
                "STG.API", #Nome objeto
                f"STG.{self.stage_include}", #Nome tabela
                self.P_HORA_INICIAL,
                self.P_HORA_FINAL,
                self.P_PERIODO_INICIAL,
                self.P_PERIODO_FINAL,
                self.P_TOTAL_REGS,
                self.P_STATUS
            ]
            db_log_cursor.callfunc("FN_INSERT_LOG_CARGA", str, log_carga_args)  #chama a função FN_INSERT_LOG_CARGA e para os parâmetros previamente definidos
        except Exception as e:
            logging.error(e)
        else:
            db_log_cursor.close()   #fecha o cursor para o CONTROLE 

    def verify(self):
        db_stg_cur = self.db_stg.cursor()   #abre um cursor para o ambiente STAGE

        stg_columns = self.db.select_columns(self.db_stg, self.stage_include) #seleciona todas as colunas do dataframe

        diferenca = []  #lista que armazenará a diferença de colunas entre a API e a STAGE (que estão presentes na API mas não estão presentes na STAGE)
        for coluna in self.df.columns:  #para cada coluna na lista de colunas do dataframe principal
            if coluna not in stg_columns:   #se a coluna não está presente nas colunas da STAGE
                diferenca.append(coluna)    #adiciona a coluna à lista de diferenças
        logging.info(f"Tabela {self.tabela_nome} com {len(diferenca)} campos diferentes: {diferenca}")

        if len(diferenca) > 0:  #caso haja diferenças
            logging.info("Inputando novos campos para tabela...")
            for coluna in diferenca:    #para cada coluna diferente
                query = f"ALTER TABLE STAGE.{self.stage_include} ADD \"{coluna}\" VARCHAR(256)" #query que adiciona coluna na tabela da STAGE, definindo o campo como varchar(256)
                db_stg_cur.execute(query) #comando que executa query
        
        db_stg_cur.close() # fecha o cursor;
    
    def linhas_df(self):
        return self.df.shape[0] #retorna a quantidade de linhas do dataframe 
    
    def csv_df(self):
        self.df.to_csv(f"/home/opc/jd_iguacu-airflow/tables/{self.tabela_nome}_df.csv") #gera um csv do dataframe


def lista_url_func(lista_dataframes, thread, numero_valores):
    lista_valores = []  #lista de valores que serão usados para compor a url
    for i in range(numero_valores): #loop que itera sobre a quantidade de valores necessários
        lista_valores.append(lista_dataframes[i][thread]) #adiciona o valor da posição i do registro iésimo para a lista de valores
    return lista_valores #retorna a lista de valores

