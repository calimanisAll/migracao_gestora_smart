import traceback
import mysql.connector
import psycopg2
import uuid
from psycopg2.extras import DictCursor
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
postgres_cursor = postgres_conn.cursor(cursor_factory=DictCursor)

try:
    mysql_cursor.execute("""
    SELECT
        
    FROM orders
    """)
    orders = mysql_cursor.fetchall()

    row_count = 0

    postgres_conn.commit()
    print(f"Total de linhas inseridas: {row_count}")

except Exception as e:
    print("Erro geral:", e)
    postgres_conn.rollback()

finally:
    mysql_cursor.close()
    mysql_conn.close()
    postgres_cursor.close()
    postgres_conn.close()