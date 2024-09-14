import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from API_Brasil import df_brasil

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

# Obter os dados do .env
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")  # Nome do banco de dados a ser criado
driver = os.getenv("DB_DRIVER")

# Verifique se todas as variáveis foram carregadas corretamente
if not all([username, password, host, driver]):
    raise ValueError("Uma ou mais variáveis de conexão não foram encontradas no .env")

# Certifique-se de que o driver seja uma string
if not isinstance(driver, str):
    raise ValueError("O valor do driver deve ser uma string")

# Criar a URL de conexão com o banco de dados (sem especificar o banco inicialmente)
master_url = URL.create(
    "mssql+pyodbc",
    username=username,
    password=password,
    host=host,
    query={"driver": driver}
)

# Conectar ao SQL Server sem especificar o banco de dados
engine = create_engine(master_url)

# Criar o banco de dados, se não existir
with engine.connect() as conn:
    conn.execute(text(f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{database}') "
                      f"BEGIN CREATE DATABASE [{database}] END;"))
    print(f"Banco de dados '{database}' verificado/criado com sucesso!")

# Atualizar a URL de conexão para incluir o banco de dados recém-criado
database_url = URL.create(
    "mssql+pyodbc",
    username=username,
    password=password,
    host=host,
    database=database,
    query={"driver": driver}
)

# Conectar ao banco de dados específico
engine = create_engine(database_url)

# Enviar o DataFrame para o banco de dados e criar a tabela, se não existir
df_brasil.to_sql('bancos_brasil', con=engine, if_exists='replace', index=False)
print("Dados enviados para o banco de dados com sucesso!")
