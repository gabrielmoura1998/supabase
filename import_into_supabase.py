import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import io

# Carregando variáveis do arquivo .env
load_dotenv()
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Carregando dataframes
arquivo = fr"C:\Users\MSI\Desktop\supabase\deputies_dataset.csv" #Insira o seu diretório do CSV
df = pd.read_csv(arquivo)

# Observação: o Pandas carrega vários tipos de arquivo, qualquer um servirá.
# No entanto, verifica se esse dataframe está normalizado ou não, pois o Supabase é um banco relacional e não NoSQL

# Conectar
try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    print("Connection successful!")

    # Cursor
    cursor = connection.cursor()

    # Criando schema de tabelas
    def create_table_from_dataframe(df, table_name):
        # Criando o código SQL para criar o schema das tabelas
        columns = []
        for col_name, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "FLOAT"
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "TIMESTAMP"
            elif pd.api.types.is_object_dtype(dtype):
                if isinstance(df[col_name].iloc[0], dict):
                    sql_type = "JSONB"
                else:
                    sql_type = "TEXT"
            else:
                sql_type = "TEXT"

            if col_name == "id":
                columns.append(f"{col_name} {sql_type} PRIMARY KEY")
            else:
                columns.append(f"{col_name} {sql_type}")

        create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
        print(f"Creating table with SQL: {create_table_sql}")

        # O cursor executa o script SQL da função anterior
        cursor.execute(create_table_sql)
        connection.commit()
        print(f"Tabela '{table_name}' criada com sucesso!")

    # Inserir os dados do dataframe na tabela do Supabase
    def copy_dataframe_to_table(df, table_name):
        # Criar um buffer para guardar os dados do CSV
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # Inserir dados via COPY
        try:
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)
            connection.commit()
            print(f"Dados inseridos na tablea '{table_name}' com sucesso!")
        except Exception as e:
            print(f"Erro: {e}")

    # Executando o processo
    TABLE_NAME = "teste2" # Nome da tabela que você deseja criar
    create_table_from_dataframe(df, TABLE_NAME)
    copy_dataframe_to_table(df, TABLE_NAME)

    # Fechando
    cursor.close()
    connection.close()
    print("Saindo do supabase...")

except Exception as e:
    print(f"Erro na conexão: {e}")
