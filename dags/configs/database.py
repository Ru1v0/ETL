import logging
import oracledb as db
#import config as cfg
from dags.configs import config as cfg


class Autonomous():
    """
        ORACLE DATABASE AUTONOMOUS ADW
    """
    def __init__(self):
        try:
            db.init_oracle_client()
        except Exception as e:
            logging.CRITICAL('Autonomous error: %e', e)

    
    def conect(self, user='STG'):
        try: 
            if user.upper() == 'STG':
                DB_USER = cfg.DB_STG_USER
                DB_PASS = cfg.DB_STG_PASS
            elif user.upper() == 'DW':
                DB_USER = cfg.DB_DW_USER
                DB_PASS = cfg.DB_DW_PASS
            elif user.upper() == 'LOG':
                DB_USER = cfg.DB_CTRL_USER
                DB_PASS = cfg.DB_CTRL_PASS

            DB_DSN  = cfg.ADW_DSN
            DB_CFG  = cfg.ADW_CFG # ADW_CFG_LOCAL

            self.con = db.connect(
                user = DB_USER,
                password = DB_PASS,
                dsn = DB_DSN,
                config_dir = DB_CFG,
                wallet_location = DB_CFG
            )

            logging.info(f"Autonomous {user} conected")
        except Exception as e:
            logging.error(f"Autonomous {user} connection error {e}")
            
        return self.con


    def execute(self, con, sql_script):
        """
            con     = conexão com o database; 
            sql_script   = script SQL a ser executado, separar comandos por ;. 
        """
        try:
            # Divide o script em comandos individuais (assumindo que os comandos estão separados por ponto e vírgula)
            sql_commands = sql_script.split(';')
            
            # Remove possíveis linhas em branco
            sql_commands = [cmd.strip() for cmd in sql_commands if cmd.strip()]

            # Cria um cursor para executar os comandos SQL
            cur = con.cursor()

            # Executa cada comando SQL individualmente
            for command in sql_commands:
                try:
                    cur.execute(command)
                    print(f"Comando executado: {command}")
                except Exception as e:
                    error, = e.args
                    print(f"Erro ao executar comando: {command}\nErro: {error}")

            # Confirma as alterações e fecha a conexão
            con.commit()
            con.close()

        except Exception as e:
            logging.error('Erro ao tentar selecionar a tabela %s: %s', sql_script, e)


    def select(self, con, table):
        """
            con     = conexão com o database; 
            table   = nome da tabela;
        """
        try:
            cur = con.cursor()
            sqlTxt = f'SELECT DISTINCT * FROM {table}'
            cur.execute(sqlTxt)
            columns = [x[0] for x in cur.description]
            values = cur.fetchall()
            cur.close()
        except Exception as e:
            logging.error('Erro ao tentar selecionar a tabela %s: %s', table, e)
        
        return columns, values
    
    def select_columns(self, con, table):
        """
            con     = conexão com o database; 
            table   = nome da tabela;
        """
        try:
            cur = con.cursor()
            sqlTxt = f'SELECT * FROM {table}'
            cur.execute(sqlTxt)
            columns = [x[0] for x in cur.description]
            cur.close()
        except Exception as e:
            logging.error('Erro ao tentar selecionar a tabela %s: %s', table, e)
        
        return columns
    
    def insert(self, con, cur, table, dataframe):
        """
            Insert data into Autonomous Database;
            Args:
                table: tabela que recebera os dados
                dataframe: dataframque sera inserido na tabela;

            Return: Dataframe
        """

        data = [tuple(x) for x in dataframe.values]
        vlrs = ":v" + f",:v".join([str(i) for i in range(len(data[0]))])
        cols = '","'.join([str(i) for i in dataframe.columns])
        sqlTxt = 'INSERT INTO '+ table +'("'+ cols +'") VALUES ('+ vlrs +')'

        cur.executemany(sqlTxt, data)
        con.commit()
    
    def clear(self, con, table, where=''):
        """
            Truncate table
        """
        try:
            cur = con.cursor()

            if where:
                sql_txt = f"DELETE FROM {table} WHERE {where}"
            else:
                sql_txt = f"TRUNCATE TABLE {table}"
            
            cur.execute(sql_txt)
            con.commit()
            logging.info(f'Dados da tabela {table} foram limpos')
        except Exception as e:
            logging.error(f'Erro ao limpar a tabela {table}: {e}')


    def function(self, cur, name, args=[]):
        """
            Call function
        """
        try:
            result = cur.callfunc(name, str, args)
            logging.info('Função %s executada, args: %s', name, args)

        except Exception as e:
            logging.error('Função %s com erro, args: %s / %s', name, args, e)

        return result
    
    def update_dw(self, con):
        # Dimensoes
        with open('dw_dimensoes.sql', 'r') as sql_file:
            sql_script = sql_file.read()
            self.execute(con, sql_script)

        # Fatos
        with open('dw_fatos.sql', 'r') as sql_file:
            sql_script = sql_file.read()
            self.execute(con, sql_script)