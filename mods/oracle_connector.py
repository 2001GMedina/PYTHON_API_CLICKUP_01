from dotenv import load_dotenv
import pyodbc
import os

load_dotenv()

# Daba base variables
dsn = os.getenv('DSN')
user = os.getenv('USER')
password = os.getenv('PASSWORD')

# Oracle DB conection function
def db_connection():
    try:
        data_connection = f"DSN={dsn};UID={user};PWD={password}"
        connection = pyodbc.connect(data_connection)
        print("Banco de dados conectado com sucesso!")
        return connection
    except:
        print("Erro ao se conectar com banco de dados!")
        raise
