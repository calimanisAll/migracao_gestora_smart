import random
from datetime import datetime
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

# Mapeamento para as operadoras
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
    "Sierra": 25
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
        # Remover caracteres não numéricos, exceto ponto e vírgula
        value = ''.join(filter(lambda x: x.isdigit()
                        or x in [',', '.'], value))
        # Substituir vírgulas por pontos (formato padrão para PostgreSQL)
        value = value.replace(',', '.')
        # Se o valor ainda não for válido, retorne None
        try:
            return float(value)
        except ValueError:
            return None
    return value  # Retorna diretamente se já for um número ou None


try:
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
            # Consultar o ID do vendedor no PostgreSQL
            postgres_cursor.execute("""
                SELECT id FROM users WHERE username = %s
            """, (contract['vendor_contract'],))
            user_result = postgres_cursor.fetchone()

            null_uuid = str(uuid.UUID("00000000-0de0-0c0f-0000-fdf0cfb00000"))
            vendor_id = user_result['id'] if user_result else null_uuid

            # Traduzir o código da operadora usando o mapeamento
            operator_name = operator_mapping.get(contract['variable1_contract'], None)

            # Consultar o ID da franquia no PostgreSQL
            franchise_parts = contract['variable2_contract'].split(" ", 1)
            franchise_value = franchise_parts[0] if len(franchise_parts) > 0 else None
            franchise_type = franchise_parts[1] if len(franchise_parts) > 1 else None

            postgres_cursor.execute("""
                SELECT id FROM crm_franchises 
                WHERE franchise = %s AND type = %s
            """, (franchise_value, franchise_type))
            franchise_result = postgres_cursor.fetchone()

            franchise_id = franchise_result['id'] if franchise_result else None

            # Limpar valores numéricos
            mensalidade = clean_numeric(contract['variable4_contract'])
            ativacao = clean_numeric(contract['variable5_contract'])
            substituicao = clean_numeric(contract['variable6_contract'])
            cancelamento = clean_numeric(contract['variable7_contract'])
            mb_excedente = clean_numeric(contract['variable8_contract'])
            cobranca = clean_numeric(contract['variable9_contract'])

            # Converter loyaty_contract para booleano
            loyaty = convert_to_boolean(contract['loyaty_contract'])

            # Converter status de aprovação usando o mapeamento
            allcom_approval_status = approval_status_mapping.get(contract['aproved_allcom_contract'], 1)
            financial_approval_status = approval_status_mapping.get(contract['aproved_financial_contract'], 1)

            # Inserir na tabela crm_contracts
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
                contract['id_contract'],  # ID
                contract['created_at_contract'],  # Created At
                vendor_id,  # Vendedor
                contract['client_contract'],  # Cliente
                contract['type_contract'],  # Type Contract
                operator_name,  # Operadora
                franchise_id,  # Franquia
                contract['variable3_contract'],  # Moeda
                mensalidade,  # Mensalidade
                ativacao,  # Ativação
                substituicao,  # Substituição
                cancelamento,  # Cancelamento
                mb_excedente,  # MB Excedente
                cobranca,  # Cobrança
                loyaty,  # Fidelidade
                allcom_approval_status,  # Fase Aprovação
                contract['date_aproved_allcom_contract'],  # Commercial Approved At
                financial_approval_status,  # Fase Aprovação Cliente
                contract['date_aproved_client_contract'],  # Customer Approved At
                contract['obs_contract'],  # Observações
                contract['last_update_contract'] or datetime.now()  # Updated At
            ))

            row_count += 1
            print(f"Linha {row_count} inserida com sucesso. ID do contrato: {contract['id_contract']}")

        except Exception as inner_e:
            print("Erro interno:", inner_e)
            print(f"ID do contrato com erro: {contract.get('id_contract', 'N/A')}")
            for key, value in contract.items():
                print(f"Coluna: {key}, Valor: {value}")
            postgres_conn.rollback()

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