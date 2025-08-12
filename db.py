from hdbcli import dbapi
import os
from dotenv import load_dotenv
load_dotenv() 

def get_conn():   

    HANA_HOST = os.getenv("HANA_HOST")
    HANA_PORT = int(os.getenv("HANA_PORT"))
    HANA_USER = os.getenv("HANA_USER")
    HANA_PWD = os.getenv("HANA_PWD")

    global_conn = dbapi.connect(
            address=HANA_HOST,
            port=HANA_PORT,
            user=HANA_USER,
            password=HANA_PWD,
            encrypt=True,
            sslValidateCertificate=False
        )
    return global_conn


def query_table_names():
    HANA_SCHEMA = os.getenv("HANA_SCHEMA")

    try:
        HANA_HOST = os.getenv("HANA_HOST")
        HANA_PORT = int(os.getenv("HANA_PORT"))
        HANA_USER = os.getenv("HANA_USER")
        HANA_PWD = os.getenv("HANA_PWD")
        
        conn = dbapi.connect(
                address=HANA_HOST,
                port=HANA_PORT,
                user=HANA_USER,
                password=HANA_PWD,
                encrypt=True,
                sslValidateCertificate=False
            )        
        cursor = conn.cursor()
        cursor.execute(f"SELECT TABLE_NAME FROM TABLES WHERE SCHEMA_NAME = '{HANA_SCHEMA}'")        
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        return [("Erro:", str(e))]

def query_table_data(table):
    try:
        conn = get_conn()
        cursor = conn.cursor()
        schema_table = '"INITIAL".'+ '"' + table + '"' 
        cursor.execute(f'SELECT top 10 * FROM {schema_table}')

        column_names = [desc[0] for desc in cursor.description]
        column_values = [list(row) for row in cursor.fetchall()]
        tabela = []
        tabela.append({ 
                "table_name" :  table,          
                "column_names": column_names,
                "column_values": column_values
            })
        cursor.close()
        conn.close()
        return tabela
    except Exception as e:
        print(e)
        return [("Erro:", str(e))]
    

def current_schema():
    conn = get_conn()
    cursor = conn.cursor()
        
    cursor.execute("SELECT CURRENT_SCHEMA FROM DUMMY")
    schema_name = cursor.fetchone()[0]
    return schema_name
    