from datetime import datetime
import random
import traceback
import mysql.connector
import psycopg2
from psycopg2.extras import DictCursor
import psycopg2.extras
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# Configurações do MySQL e PostgreSQL
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
    # Verificar se a operadora "Oi" já existe
    mysql_cursor.execute("SELECT id FROM operators WHERE name = 'Oi'")
    oi_operator = mysql_cursor.fetchone()

    if not oi_operator:
        # Inserir a operadora "Oi"
        mysql_cursor.execute("""
            INSERT INTO operators (name, created_at)
            VALUES (%s, %s)
        """, ('Oi', datetime.now()))
        mysql_conn.commit()
        print("Operadora 'Oi' adicionada com sucesso no MySQL.")
    else:
        print("Operadora 'Oi' já existe no MySQL.")

    # Verificar se a operadora "Algar" já existe
    mysql_cursor.execute("SELECT id FROM operators WHERE name = 'Algar'")
    algar_operator = mysql_cursor.fetchone()

    if not algar_operator:
        # Inserir a operadora "Algar"
        mysql_cursor.execute("""
            INSERT INTO operators (name, created_at)
            VALUES (%s, %s)
        """, ('Algar', datetime.now()))
        mysql_conn.commit()
        print("Operadora 'Algar' adicionada com sucesso no MySQL.")
    else:
        print("Operadora 'Algar' já existe no MySQL.")

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
    auto_id_simcard = 1  # Inicialização do ID autoincrementado para crm_operators_simcard_cut

    for operator in operators:
        # Definir moeda e valor do SMS
        coin = "R$" if operator['name'] != "Sierra" else "U$"
        sms = round(random.uniform(0.50, 2.50), 2)

        # Inserir operador em crm_operators
        postgres_cursor.execute("""
            INSERT INTO crm_operators (
                id,
                name,
                coin,
                sms,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            operator['id'],
            operator['name'],
            coin,
            sms,
            operator['created_at'],
            datetime.now()
        ))

        # Associar todos os simcard_cut_ids ao operador
        for simcard_cut_id in simcard_cut_ids:
            postgres_cursor.execute("""
                INSERT INTO crm_operators_simcard_cut (
                    id,
                    operator_id,
                    simcard_cut_id
                )
                VALUES (%s, %s, %s)
            """, (
                auto_id_simcard,
                operator['id'],
                simcard_cut_id
            ))
            auto_id_simcard += 1

        row_count_operators += 1
        print(f"Operadora {row_count_operators} inserida com sucesso. ID: {operator['id']}")

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
    auto_id_franchise = 1

    for operator in operators:
        for franchise in franchises:
            # Inserir franquia em crm_franchises
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
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                auto_id_franchise,
                operator['id'],
                franchise['franchise'],
                franchise['type'],
                True,
                franchise['created_at'],
                datetime.now()
            ))
            auto_id_franchise += 1
            row_count_franchises += 1
            print(f"Franquia {row_count_franchises} inserida com sucesso. Operador: {operator['id']}, Franquia: {franchise['franchise']}")

    # Adicionar franquias 100GB VOZ ILIMITADO e 50GB VOZ ILIMITADO
    for operator in operators:
        for franchise_name in ["100GB VOZ ILIMITADO", "50GB VOZ ILIMITADO"]:
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
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                auto_id_franchise,
                operator['id'],
                franchise_name,
                "default",
                True,
                datetime.now(),
                datetime.now()
            ))
            auto_id_franchise += 1
            row_count_franchises += 1
            print(f"Franquia adicional '{franchise_name}' inserida com sucesso para operador {operator['id']}")

    # Criar operadora "N/A"
    postgres_cursor.execute("SELECT MAX(id) FROM crm_operators")
    result = postgres_cursor.fetchone()
    next_operator_id = (result[0] + 1) if result and result[0] is not None else 1

    postgres_cursor.execute("""
        INSERT INTO crm_operators (
            id,
            name,
            coin,
            sms,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        next_operator_id,
        "N/A",
        "R$",
        0.00,
        datetime.now(),
        datetime.now()
    ))
    print("Operadora N/A inserida com sucesso.")

    # Criar franquia "N/A"
    postgres_cursor.execute("SELECT MAX(id) FROM crm_franchises")
    result = postgres_cursor.fetchone()
    next_franchise_id = (result[0] + 1) if result and result[0] is not None else 1

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
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        next_franchise_id,
        next_operator_id,
        "N/A",
        "default",
        True,
        datetime.now(),
        datetime.now()
    ))
    print("Franquia N/A inserida com sucesso.")

    # Confirmar alterações no banco
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

