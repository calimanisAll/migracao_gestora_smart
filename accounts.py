from datetime import datetime
import traceback
import mysql.connector
import psycopg2
import uuid
from psycopg2.extras import execute_values
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

try:
    mysql_cursor.execute("""
    SELECT
        id_account,
        created_at_account,
        vendor_account,
        company_name_account,
        fantasy_name_account,
        cnpj_cpf_account,
        type_account,
        phase_account,
        source_account,
        rank_account,
        billing_date_account,
        street_account,
        number_account,
        complement_account,
        cep_account,
        uf_account,
        country_account,
        city_account,
        name_account,
        email_account,
        phone_account,
        obs_account,
        obs_lead_account,
        phase_lead_account,
        lead_type_account,
        date_prospect_account,
        date_conquered_account,
        social_capital_account,
        black_list_account,
        bloq_sms_account,
        updated_at_account
    FROM accounts
    """)
    accounts = mysql_cursor.fetchall()

    # Mapeamento para as fases
    phase_mapping = {
        'Prospect': 2,
        'Conquistado': 3,
        'Lead': 1
    }

    phase_lead_mapping = {
        'Convertido': 7,
        'Perdido': 4,
        'Contato Pendente': 1,
        'Proposta': 3,
        'Negociação': 5,
        'Contato Iniciado': 2
    }

    row_count = 0  # Inicializa o contador de linhas

    for account in accounts:
        # Conversão de black_list_account e bloq_sms_account
        black_list = True if account['black_list_account'] == "Sim" else False
        bloq_sms = True if account['bloq_sms_account'] == "Sim" else False

        # Mapeamento da fase
        phase_id = phase_mapping.get(account['phase_account'], 1)
        phase_lead_id = phase_lead_mapping.get(
            account['phase_lead_account'], 1)

        # Consultar o ID do usuário no PostgreSQL
        postgres_cursor.execute("""
            SELECT id FROM users WHERE username = %s
        """, (account['vendor_account'],))
        user_result = postgres_cursor.fetchone()

        null_uuid = str(uuid.UUID("00000000-0de0-0c0f-0000-fdf0cfb00000"))

        vendor_id = user_result['id'] if user_result else null_uuid

        # Inserindo na tabela customers
        postgres_cursor.execute("""
            INSERT INTO customers (id, created_at, razao_social, name, cpf_cnpj, vertical, blacklist, bloqueio_sms, email, telefone, updated_at)
            VALUES (%s, %s, %s, COALESCE(%s, %s), %s, %s, %s, %s, %s, %s, %s)
        """, (
            account['id_account'],
            account['created_at_account'],
            account['company_name_account'],
            account['name_account'],
            account['fantasy_name_account'],
            account['cnpj_cpf_account'],
            account['type_account'],
            black_list,
            bloq_sms,
            account['email_account'],
            account['phone_account'],
            account['updated_at_account'] or datetime.now()
        ))

        # Inserindo na tabela info_customers
        postgres_cursor.execute("""
            INSERT INTO info_customers (id_customer, responsible_seller_id, status_id, source, rank, billing_date, obs, obs_lead, phase_id, type, date_prospect, date_conquered, social_capital)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            account['id_account'],
            vendor_id,
            phase_id,
            account['source_account'],
            account['rank_account'],
            account['billing_date_account'],
            account['obs_account'],
            account['obs_lead_account'],
            phase_lead_id,
            account['lead_type_account'],
            account['date_prospect_account'] or None,
            account['date_conquered_account'] or None,
            account['social_capital_account'] or None,
        ))

        # Inserindo na tabela address_customers
        postgres_cursor.execute("""
            INSERT INTO address_customers (id_customer, logradouro, logradouro_numero, complemento, cep, estado, pais, cidade, bairro, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            account['id_account'],
            account['street_account'] or "",
            account['number_account'] or "",
            account['complement_account'] or "",
            account['cep_account'] or "",
            account['uf_account'] or "",
            account['country_account'] or "",
            account['city_account'] or "",
            "",
            account['created_at_account'],
            account['updated_at_account'] or datetime.now()

        ))

        # Incrementa o contador e imprime o progresso
        row_count += 1
        print(f"Linha {row_count} inserida com sucesso." + "ID: " + str(account['id_account']))

    postgres_conn.commit()
    print(f"Total de linhas inseridas: {row_count}")

except mysql.connector.Error as mysql_error:
    print("ID do erro: " + str(account['id_account']))
    print(f"Erro no MySQL: {mysql_error}")
    print(traceback.format_exc())
    postgres_conn.rollback()
    print("Rollback realizado no PostgreSQL.")

except psycopg2.Error as postgres_error:
    print("ID do erro: " + str(account['id_account']))
    print(f"Erro no PostgreSQL: {postgres_error}")
    print(traceback.format_exc())
    postgres_conn.rollback()
    print("Rollback realizado no PostgreSQL.")

except Exception as e:
    print("ID do erro: " + str(account['id_account']))
    print(f"Erro inesperado: {e}")
    print(traceback.format_exc())
    postgres_conn.rollback()
    print("Rollback realizado no PostgreSQL.")

finally:
    mysql_cursor.close()
    mysql_conn.close()
    postgres_cursor.close()
    postgres_conn.close()
