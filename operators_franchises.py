from datetime import datetime
import random
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

try:
    # Inserção de Operadoras
    mysql_cursor.execute("""
    SELECT
        id,
        name,
        created_at
    FROM operators
    """)
    operators = mysql_cursor.fetchall()

    row_count_operators = 0
    simcard_cut_ids = [1, 2, 3, 4]  # IDs dos cortes de SIM card
    # Inicialização do ID autoincrementado para a tabela crm_operator_simcard_cut
    auto_id_simcard = 1

    for operator in operators:
        # Definir a moeda com base no nome do operador
        coin = "R$" if operator['name'] != "Sierra" else "U$"

        # Gerar um valor aleatório para SMS entre 0.50 e 2.50
        sms = round(random.uniform(0.50, 2.50), 2)

        # Inserir na tabela crm_operators
        postgres_cursor.execute("""
            INSERT INTO crm_operators (
                id,
                name,
                coin,
                sms,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s
            )
        """, (
            operator['id'],          # ID
            operator['name'],        # Nome
            coin,                    # Moeda
            sms,                     # Valor do SMS
            operator['created_at'],  # Data de criação
            datetime.now()           # Data de atualização
        ))

        # Associar todos os simcard_cut_ids ao operador
        for simcard_cut_id in simcard_cut_ids:
            postgres_cursor.execute("""
                INSERT INTO crm_operators_simcard_cut (
                    id,
                    operator_id,
                    simcard_cut_id
                )
                VALUES (
                    %s, %s, %s
                )
            """, (
                auto_id_simcard,       # ID autoincrementado
                operator['id'],        # ID do operador
                simcard_cut_id         # ID do corte do SIM card
            ))
            auto_id_simcard += 1

        row_count_operators += 1
        print(f"Operadora {row_count_operators} inserida com sucesso. ID: {
              operator['id']}")

    # Inserção de Franquias
    mysql_cursor.execute("""
    SELECT
        id,
        franchise,
        type,
        created_at
    FROM franchises
    """)
    franchises = mysql_cursor.fetchall()

    row_count_franchises = 0
    auto_id_franchise = 1  # Inicialização do ID autoincrementado para a tabela crm_franchises

    for operator in operators:
        for franchise in franchises:
            # Inserir cada combinação de operador e franquia na tabela crm_franchises
            postgres_cursor.execute("""
                INSERT INTO crm_franchises (
                    id,
                    operator_id,
                    franchise,
                    type,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                auto_id_franchise,     # ID autoincrementado
                operator['id'],        # ID do operador
                franchise['franchise'],  # Nome da franquia
                franchise['type'],     # Tipo da franquia
                True,                  # Status ativo
                franchise['created_at'],  # Data de criação
                datetime.now()         # Data de atualização
            ))
            auto_id_franchise += 1

            row_count_franchises += 1
            print(f"Franquia {row_count_franchises} inserida com sucesso. Operador: {
                  operator['id']}, Franquia: {franchise['franchise']}")

    postgres_conn.commit()
    print(f"Total de operadoras inseridas: {row_count_operators}")
    print(f"Total de franquias inseridas: {row_count_franchises}")

except mysql.connector.Error as mysql_error:
    print("Erro no MySQL.")
    print(f"Erro: {mysql_error}")
    print(traceback.format_exc())
    postgres_conn.rollback()
    print("Rollback realizado no PostgreSQL.")

except psycopg2.Error as postgres_error:
    print("Erro no PostgreSQL.")
    print(f"Erro: {postgres_error}")
    print(traceback.format_exc())
    postgres_conn.rollback()
    print("Rollback realizado no PostgreSQL.")

except Exception as e:
    print("Erro inesperado.")
    print(f"Erro: {e}")
    print(traceback.format_exc())
    postgres_conn.rollback()
    print("Rollback realizado no PostgreSQL.")

finally:
    mysql_cursor.close()
    mysql_conn.close()
    postgres_cursor.close()
    postgres_conn.close()
