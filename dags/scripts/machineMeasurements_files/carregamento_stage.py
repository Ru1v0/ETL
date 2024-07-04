from dags.configs.database import Autonomous
from oracledb import DatabaseError
import logging


def inserir_database(total_regs, df_maquinas_total, TPO_EXEC, intervalos):
    db = Autonomous()
    db_stg = db.conect("STG")
    db_stg_cursor = db_stg.cursor()

    try:
        db_stg_cursor = db_stg.cursor()    #abre um cursor para stage
        if df_maquinas_total.shape[0] > 0:    #caso o número de registros do dataframe seja maior que 0
            if TPO_EXEC == "DIA":
                db.clear(db_stg, "STAGE.RAW_DATA_HISTORICO_MEASUREMENT_MACHINES", where = f"\"startDate\" = '{intervalos[0][0]}' AND \"endDate\" = '{intervalos[0][1]}'")

            db.insert(db_stg, db_stg_cursor, "STAGE.RAW_DATA_HISTORICO_MEASUREMENT_MACHINES", df_maquinas_total.astype(str)) #insere a tabela na stage
            total_regs += df_maquinas_total.shape[0]

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

        logging.info(f"Tabela Historico Measurement Machines inserida com sucesso no Autonomous com destino à tabela STAGE.RAW_DATA_HISTORICO_MEASUREMENT_MACHINES, contendo {df_maquinas_total.shape[0]} registros")

    except Exception as e:
        logging.error(e)

    else:   
        db_stg_cursor.close() #fecha o cursor da stage
    
    return total_regs