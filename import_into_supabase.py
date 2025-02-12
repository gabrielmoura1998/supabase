import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import io

# Load environment variables from .env
load_dotenv()

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Load DataFrame from CSV
arquivo = fr"C:\Users\MSI\Desktop\supabase\deputies_dataset.csv"
df = pd.read_csv(arquivo)

# Step 1: Connect to the database
try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    print("Connection successful!")

    # Create a cursor to execute SQL queries
    cursor = connection.cursor()

    # Step 2: Dynamically create table schema
    def create_table_from_dataframe(df, table_name):
        # Generate the SQL CREATE TABLE statement
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
                # Check if the column contains dictionaries (e.g., JSON)
                if isinstance(df[col_name].iloc[0], dict):
                    sql_type = "JSONB"
                else:
                    sql_type = "TEXT"
            else:
                sql_type = "TEXT"  # Default to TEXT for unknown types

            if col_name == "id":  # Assume 'id' is the primary key
                columns.append(f"{col_name} {sql_type} PRIMARY KEY")
            else:
                columns.append(f"{col_name} {sql_type}")

        create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
        print(f"Creating table with SQL: {create_table_sql}")

        # Execute the CREATE TABLE statement
        cursor.execute(create_table_sql)
        connection.commit()
        print(f"Table '{table_name}' created successfully!")

    # Step 3: Insert DataFrame data into the table
    def copy_dataframe_to_table(df, table_name):
        # Create a string buffer to hold the CSV data
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # Use COPY to load the data
        try:
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)
            connection.commit()
            print(f"Data copied into table '{table_name}' successfully!")
        except Exception as e:
            print(f"Failed to copy data: {e}")

    # Step 4: Run the process
    TABLE_NAME = "teste2"  # Replace with your desired table name
    create_table_from_dataframe(df, TABLE_NAME)
    copy_dataframe_to_table(df, TABLE_NAME)

    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("Connection closed.")

except Exception as e:
    print(f"Failed to connect or execute queries: {e}")