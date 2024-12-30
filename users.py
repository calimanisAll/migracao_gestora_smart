from datetime import datetime
import time
import mysql.connector
import psycopg2
import uuid
from dotenv import load_dotenv
import os

load_dotenv()

# Configurações de conexão
mysql_config = {
    'host': os.getenv('MYSQL_DATABASE_HOST'),
    'user': os.getenv('MYSQL_DATABASE_USER'),
    'password': os.getenv('MYSQL_DATABASE_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
}

postgres_config = {
    'host': os.getenv('POSTGRES_DATABASE_HOST'),
    'user': os.getenv('POSTGRES_DATABASE_USER'),
    'password': os.getenv('POSTGRES_DATABASE_PASSWORD'),
        'database': os.getenv('POSTGRES_DATABASE'),
    'port': os.getenv('POSTGRES_DATABASE_PORT'),
    'sslmode': 'require',
}

# Conexões com bancos
mysql_conn = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor(dictionary=True)

postgres_conn = psycopg2.connect(**postgres_config)
postgres_cursor = postgres_conn.cursor()

try:
    start_time = time.time()
    # Obtém dados do MySQL
    mysql_cursor.execute(
        "SELECT id_user, name_user, email_user, username, password, created_at, updated_at FROM users")
    users = mysql_cursor.fetchall()

    for user in users:
        uuid_id = str(uuid.uuid4())
        name = user['name_user']
        email = user['email_user']
        username = user['username']
        password = user['password']
        created_at = user['created_at']
        updated_at = user['updated_at'] or datetime.now()
        account_type = "smartsim"
        reference_user = 1

        # Insere apenas se email e username não existirem
        postgres_cursor.execute("""
            INSERT INTO users (id, name, email, username, password, account_type, reference_user, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
        """, (uuid_id, name, email, username, password, account_type, reference_user, created_at, updated_at))

    # Confirma transações
    postgres_conn.commit()
    print(f"{len(users)} registros processados com sucesso.")
    
    end_time = time.time() 
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")

except Exception as e:
    # Em caso de erro, realiza rollback
    print(f"Erro durante a migração: {e}")
    postgres_conn.rollback()
    print("Rollback realizado.")

finally:
    # Fecha conexões
    mysql_cursor.close()
    mysql_conn.close()
    postgres_cursor.close()
    postgres_conn.close()
