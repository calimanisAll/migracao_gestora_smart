import random
from datetime import datetime
import time
import traceback
import mysql.connector
import psycopg2
import uuid
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
import os

load_dotenv()

# Configuração dos bancos de dados
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

# Mapeamentos
operator_mapping = {
    "Vivo": 1,
    "Vivo (Pública)": 1,
    "Claro": 2,
    "Tim": 3,
    "Tim (Pública)": 3,
    "Algar 5 Operadoras": 6,
    "Vivo Catm1": 11,
    "Arqia Internacional": 15,
    "Claro Internacional": 17,
    "Arqia (TIM + VIVO)": 18,
    "Vivo BL": 19,
    "VIVO VOZ": 21,
    "Claro BL": 22,
    "Algar 3 Operadoras": 23,
    "Claro CATM1": 24,
    "Sierra": 25,
    "Oi": 26,
    "OI": 26,
}

approval_status_mapping = {
    "Pendente": 1,
    "Recusado": 2,
    "Aprovado": 3
}

mysql_conn = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor(dictionary=True)

postgres_conn = psycopg2.connect(**postgres_config)
postgres_cursor = postgres_conn.cursor(cursor_factory=DictCursor)


def convert_to_numeric(value):
    if isinstance(value, str):
        return value.replace(',', '.')
    return value


def convert_to_boolean(value):
    return value.strip().lower() == 'sim' if isinstance(value, str) else False


def clean_numeric(value):
    if isinstance(value, str):
        value = ''.join(filter(lambda x: x.isdigit()
                        or x in [',', '.'], value))
        value = value.replace(',', '.')
        try:
            return float(value)
        except ValueError:
            return None
    return value


try:
    start_time = time.time()
    # Obter o ID da operadora "N/A"
    postgres_cursor.execute("""
        SELECT id FROM crm_operators WHERE name = %s
    """, ("N/A",))
    na_operator_result = postgres_cursor.fetchone()
    na_operator_id = na_operator_result['id'] if na_operator_result else None

    # Obter o ID da franquia "N/A"
    postgres_cursor.execute("""
        SELECT id FROM crm_franchises WHERE franchise = %s AND operator_id = %s
    """, ("N/A", na_operator_id))
    na_franchise_result = postgres_cursor.fetchone()
    na_franchise_id = na_franchise_result['id'] if na_franchise_result else None

    if not na_operator_id or not na_franchise_id:
        raise Exception(
            "A operadora ou franquia 'N/A' não foi encontrada no banco de dados.")

    # Buscar contratos no MySQL
    mysql_cursor.execute("""
    SELECT
        id_contract,
        created_at_contract,
        vendor_contract,
        client_contract,
        type_contract,
        variable1_contract,
        variable2_contract,
        variable3_contract,
        variable4_contract,
        variable5_contract,
        variable6_contract,
        variable7_contract,
        variable8_contract,
        variable9_contract,
        loyaty_contract,
        aproved_allcom_contract,
        aproved_financial_contract,
        date_aproved_allcom_contract,
        date_aproved_financial_contract,
        date_aproved_client_contract,
        obs_contract,
        last_update_contract
    FROM contracts
    """)
    contracts = mysql_cursor.fetchall()

    row_count = 0

    for contract in contracts:
        try:
            # Buscar o ID do vendedor
            postgres_cursor.execute("""
                SELECT id FROM users WHERE username = %s
            """, (contract['vendor_contract'],))
            user_result = postgres_cursor.fetchone()
            null_uuid = str(uuid.UUID("c531f1e9-b8b8-40e8-8efa-5bed8cdaae64"))
            vendor_id = user_result['id'] if user_result else null_uuid

            # Determinar o ID da operadora
            operator_name = operator_mapping.get(
                contract['variable1_contract'], None)
            operator_id = operator_name if operator_name else na_operator_id

            # Determinar o ID da franquia
            franchise_value, franchise_type = None, None
            if contract['variable2_contract']:
                franchise_parts = contract['variable2_contract'].split(" ", 1)
                franchise_value = franchise_parts[0] if len(
                    franchise_parts) > 0 else None
                franchise_type = franchise_parts[1] if len(
                    franchise_parts) > 1 else None

            postgres_cursor.execute("""
                SELECT id FROM crm_franchises 
                WHERE franchise = %s AND type = %s
            """, (franchise_value, franchise_type))
            franchise_result = postgres_cursor.fetchone()

            franchise_id = franchise_result['id'] if franchise_result else na_franchise_id

            # Conversão de valores numéricos
            mensalidade = clean_numeric(contract['variable4_contract'])
            ativacao = clean_numeric(contract['variable5_contract'])
            substituicao = clean_numeric(contract['variable6_contract'])
            cancelamento = clean_numeric(contract['variable7_contract'])
            mb_excedente = clean_numeric(contract['variable8_contract'])
            cobranca = clean_numeric(contract['variable9_contract'])

            # Conversão de fidelidade e status de aprovação
            loyaty = convert_to_boolean(contract['loyaty_contract'])
            allcom_approval_status = approval_status_mapping.get(
                contract['aproved_allcom_contract'], 1)
            financial_approval_status = approval_status_mapping.get(
                contract['aproved_financial_contract'], 1)

            # Inserir contrato no PostgreSQL
            postgres_cursor.execute("""
                INSERT INTO crm_contracts (
                    id,
                    created_at,
                    vendedor,
                    cliente,
                    type_contract,
                    operadora,
                    franquia,
                    moeda,
                    mensalidade,
                    ativacao,
                    substituicao,
                    cancelamento,
                    mb_excedente,
                    cobranca,
                    fidelidade,
                    fase_aprovacao,
                    commercial_approved_at,
                    fase_aprovacao_cliente,
                    customer_approved_at,
                    observacoes,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                contract['id_contract'],
                contract['created_at_contract'],
                vendor_id,
                contract['client_contract'],
                contract['type_contract'],
                operator_id,
                franchise_id,
                contract['variable3_contract'],
                mensalidade,
                ativacao,
                substituicao,
                cancelamento,
                mb_excedente,
                cobranca,
                loyaty,
                allcom_approval_status,
                contract['date_aproved_allcom_contract'],
                financial_approval_status,
                contract['date_aproved_client_contract'],
                contract['obs_contract'],
                contract['last_update_contract'] or datetime.now()
            ))

            row_count += 1
            print(f"Linha {row_count} inserida com sucesso. ID do contrato: {
                  contract['id_contract']}")

        except Exception as inner_e:
            print("Erro interno:", inner_e)
            print(f"ID do contrato com erro: {
                  contract.get('id_contract', 'N/A')}")
            postgres_conn.rollback()

    postgres_conn.commit()
    print(f"Total de linhas inseridas: {row_count}")

    end_time = time.time()
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")

except Exception as e:
    print("Erro geral:", e)
    postgres_conn.rollback()

finally:
    mysql_cursor.close()
    mysql_conn.close()
    postgres_cursor.close()
    postgres_conn.close()
