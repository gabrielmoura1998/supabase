# Código usado para se conectar ao Supabase

import psycopg2
from dotenv import load_dotenv
import os

# Carregar variáveis do .env
load_dotenv()
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Conectar ao Supabase
try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    print("Conectado!")

    # Criando cursor
    cursor = connection.cursor()

    # Teste
    cursor.execute("SELECT NOW();")
    result = cursor.fetchone()
    print("Tempo atual:", result)

    # Fechando
    cursor.close()
    connection.close()
    print("Conexão encerrada.")

except Exception as e:
    print(f"Erro: {e}")
