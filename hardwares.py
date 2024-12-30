from datetime import datetime
import random
import time
import traceback
import mysql.connector
import psycopg2
from psycopg2.extras import DictCursor
import psycopg2.extras

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
    'port': os.getenv('POSTGRES_DATABASE_PORT'),
    'sslmode': 'require',
}

mysql_conn = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor(dictionary=True)

postgres_conn = psycopg2.connect(**postgres_config)
postgres_cursor = postgres_conn.cursor(
    cursor_factory=psycopg2.extras.DictCursor)

mysql_conn = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor(dictionary=True)

postgres_conn = psycopg2.connect(**postgres_config)
postgres_cursor = postgres_conn.cursor(cursor_factory=DictCursor)

# Dicionário para mapear ID à família correta
family_mapping = {
    2: "Rastreador", 3: "Rastreador", 4: "Rastreador", 5: "Rastreador", 8: "Rastreador",
    9: "Rastreador", 10: "Rastreador", 11: "Rastreador", 12: "Rastreador", 13: "Rastreador",
    14: "Rastreador", 15: "Rastreador", 16: "Rastreador", 17: "Rastreador", 18: "Rastreador",
    19: "Rastreador", 20: "Rastreador", 21: "Acessório", 22: "Acessório", 23: "Rastreador",
    24: "Rastreador", 26: "Acessório", 28: "Rastreador", 30: "POS", 32: "POS", 33: "POS",
    34: "POS", 35: "POS", 37: "POS", 38: "POS", 39: "POS", 40: "POS", 41: "POS",
    42: "POS", 44: "Acessório", 45: "Rastreador", 46: "Rastreador", 47: "Acessório",
    49: "Rastreador", 50: "Rastreador", 52: "Rastreador", 54: "Rastreador", 55: "Rastreador",
    56: "Rastreador", 57: "Câmera", 59: "Rastreador", 60: "Acessório", 61: "POS",
    62: "Rastreador", 63: "Acessório", 64: "Acessório", 65: "Acessório", 67: "POS",
    68: "Rastreador", 69: "Rastreador", 70: "Acessório", 71: "POS", 72: "POS",
    73: "Acessório", 74: "Rastreador", 76: "Acessório", 82: "Rastreador", 83: "Rastreador",
    84: "Rastreador", 85: "Rastreador", 86: "Rastreador", 87: "Acessório", 88: "Acessório",
    89: "Rastreador", 90: "Rastreador", 91: "Rastreador", 92: "Rastreador", 93: "Rastreador",
    94: "Câmera", 95: "Câmera", 96: "Rastreador", 97: "Rastreador", 98: "Rastreador",
    99: "Rastreador", 100: "Rastreador", 101: "Rastreador"
}

try:
    start_time = time.time()

    # Obter o maior ID existente na tabela `crm_hardwares`
    

    # Inserção de hardwares existentes no MySQL
    mysql_cursor.execute("""
    SELECT
        id,
        brand,
        model,
        type,
        created_at
    FROM hardwares
    """)
    hardwares = mysql_cursor.fetchall()

    row_count_hardwares = 0

    for hardware in hardwares:
        # Determinar família com base no ID
        family = family_mapping.get(hardware['id'], "Desconhecido")

        # Inserir na tabela crm_hardwares
        postgres_cursor.execute("""
            INSERT INTO crm_hardwares (
                id,
                brand,
                model,
                sell_type,
                family,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            hardware['id'],
            hardware['brand'],
            hardware['model'],
            hardware['type'],
            family,
            hardware['created_at'],
            datetime.now()
        ))

        row_count_hardwares += 1
        print(f"Hardware {row_count_hardwares} inserido com sucesso. ID: {hardware['id']}")


    postgres_cursor.execute("SELECT MAX(id) FROM crm_hardwares")
    last_id = postgres_cursor.fetchone()[0]

    # Inserção manual dos hardwares adicionais
    manual_hardwares = [
        {"brand": "Oneblock", "model": "BMS 4G", "type": "Venda"},
        {"brand": "X3TECH", "model": "NT40", "type": "Venda"},
        {"brand": "Oneblock", "model": "BMS 2G", "type": "Venda"},
        {"brand": "Oneblock", "model": "BMS 2G v2", "type": "Venda"},
        {"brand": "NEWTEC", "model": "JC400D – CÂMERA", "type": "Venda"},
        {"brand": "Suntech", "model": "ST310U", "type": "Venda"},
        {"brand": "Suntech", "model": "ST310UC", "type": "Venda"},
        {"brand": "Suntech", "model": "ST8310", "type": "Venda"},
    ]

    for idx, hardware in enumerate(manual_hardwares, start=1):
        # Incrementa o último ID para garantir unicidade
        last_id += 1
        postgres_cursor.execute("""
            INSERT INTO crm_hardwares (
                id,
                brand,
                model,
                sell_type,
                family,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            last_id,
            hardware['brand'],
            hardware['model'],
            hardware['type'],
            "Rastreador",
            datetime.now(),
            datetime.now()
        ))
        print(f"Hardware manual {idx} inserido com sucesso. ID: {last_id}, Modelo: {hardware['model']}")

    postgres_conn.commit()
    print(f"Total de hardwares inseridos automaticamente: {row_count_hardwares}")
    print(f"Total de hardwares manuais inseridos: {len(manual_hardwares)}")

    end_time = time.time()
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")

except mysql.connector.Error as mysql_error:
    print("Erro no MySQL.")
    print(f"Erro: {mysql_error}")
    postgres_conn.rollback()
    print("Rollback realizado no PostgreSQL.")

except psycopg2.Error as postgres_error:
    print("Erro no PostgreSQL.")
    print(f"Erro: {postgres_error}")
    postgres_conn.rollback()
    print("Rollback realizado no PostgreSQL.")

except Exception as e:
    print("Erro inesperado.")
    print(f"Erro: {e}")
    postgres_conn.rollback()
    print("Rollback realizado no PostgreSQL.")

finally:
    mysql_cursor.close()
    mysql_conn.close()
    postgres_cursor.close()
    postgres_conn.close()
