from dotenv import load_dotenv
import pyodbc
import os

# carregar variáveis de ambiente
load_dotenv()

# Variáveis para acessar banco de dados
dsn = os.getenv('DSN')
user = os.getenv('USER')
password = os.getenv('PASSWORD')

# Função para conectar ao banco de dados
def db_connection():
    try:
        data_connection = f"DSN={dsn};UID={user};PWD={password}"
        connection = pyodbc.connect(data_connection)
        print("Banco de dados conectado com sucesso!")
        return connection
    except:
        print("Erro ao se conectar com banco de dados!")
        raise
