from datetime import datetime
import mysql.connector
import psycopg2
import uuid
from psycopg2.extras import execute_values
from dotenv import load_dotenv

import os

load_dotenv()

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
}

mysql_conn = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor(dictionary=True)

postgres_conn = psycopg2.connect(**postgres_config)
postgres_cursor = postgres_conn.cursor()

try:
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

        postgres_cursor.execute("""
            INSERT INTO users (id, name, email, username, password, account_type, reference_user, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
        """, (uuid_id, name, email, username, password, account_type, reference_user, created_at, updated_at))

    postgres_conn.commit()
    print(f"{len(users)} registros migrados com sucesso.")

except Exception as e:
    print(f"Erro durante a migração: {e}")
    postgres_conn.rollback()
    print("Rollback realizado.")

finally:
    mysql_cursor.close()
    mysql_conn.close()
    postgres_cursor.close()
    postgres_conn.close()
