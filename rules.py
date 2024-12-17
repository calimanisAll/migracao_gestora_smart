from datetime import datetime
import random
import traceback
import uuid
import mysql.connector
import psycopg2
from psycopg2.extras import DictCursor
import psycopg2.extras
import time

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
            id_rule,
            created_at_rule,
            user_rule,
            type_rule,
            variable1_rule,
            variable2_rule,
            variable3_rule,
            variable4_rule,
            variable5_rule,
            variable6_rule,
            variable7_rule,
            variable8_rule,
            variable9_rule
        FROM rules
    """)
    rules = mysql_cursor.fetchall()

    row_count_hw_rules = 0
    row_count_sc_rules = 0
    auto_id = 1
    for rule in rules:

        postgres_cursor.execute("""
            SELECT id FROM users WHERE username = %s
        """, (rule['user_rule'],))
        user_result = postgres_cursor.fetchone()

        null_uuid = str(uuid.UUID("00000000-0de0-0c0f-0000-fdf0cfb00000"))

        vendor_id = user_result['id'] if user_result else null_uuid

        if "Hardware" in rule.get('type_rule', ''):
            postgres_cursor.execute(
                """
                    SELECT id, brand, model
                    FROM crm_hardwares
                    WHERE brand = %s AND model LIKE CONCAT('%%', %s, '%%')
                    """,
                (rule.get('variable1_rule'), rule.get('variable2_rule'))
            )
            hardware_result = postgres_cursor.fetchone()

            preco_inicial = clean_numeric(rule['variable5_rule'])
            preco_final = clean_numeric(rule['variable9_rule'])
            possui_entrada = convert_to_boolean(rule['variable8_rule'])

            if preco_final is None:
                preco_final = preco_inicial

            quantidade = rule.get('variable6_rule') if rule.get(
                'variable6_rule') is not None else 0
            comissao = rule.get('variable6_rule') if rule.get(
                'variable6_rule') is not None else 0

            postgres_cursor.execute("""
                        INSERT INTO crm_regra_aprovacao_hardware (
                            id,
                            hardware_id,
                            preco_inicial,
                            preco_final,
                            quantidade,
                            comissao,
                            possui_entrada,
                            prazo_pagamento,
                            created_by,
                            data_criacao,
                            tipo_venda
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                auto_id,     # ID autoincrementado
                hardware_result['id'],  # ID do hardware
                preco_inicial,  # Preço inicial
                preco_final,  # Preço final
                # rule['variable6_rule'],  # Quantidade
                quantidade,
                # rule['variable7_rule'],  # Comissão
                comissao,
                possui_entrada,  # Possui entrada
                rule['variable4_rule'],  # Prazo de pagamento
                vendor_id,  # ID do vendedor
                rule['created_at_rule'],  # Data de criação
                rule['variable3_rule'],  # Tipo de venda
            ))
            auto_id += 1

            row_count_hw_rules += 1

        if "Simcard" in rule.get('type_rule', ''):
            postgres_cursor.execute(
                """
                    SELECT o.id AS operator_id, o.name AS operator_name, f.id AS franchise_id, f.franchise, f.type
                    FROM crm_operators o
                    INNER JOIN crm_franchises f ON f.operator_id = o.id
                    WHERE o.name = %s
                    AND f.franchise = %s
                    AND f.type = %s
                    """,
                (
                    rule.get('variable1_rule'),
                    # Pega a franquia (parte antes do espaço)
                    rule.get('variable2_rule').split(' ')[0],
                    # Pega o tipo (parte depois do espaço)
                    ' '.join(rule.get('variable2_rule').split(' ')[1:]),
                )
            )

            simcard_result = postgres_cursor.fetchone()
            postgres_cursor.execute("""
                        INSERT INTO crm_regra_aprovacao_simcard(
                            id_regra,
                            operadora_id,
                            franquia_id,
                            mensalidade,
                            created_at,
                            updated_at,
                            tipo,
                            usuario_criacao
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                auto_id,     # ID autoincrementado
                simcard_result['operator_id'],  # ID da operadora
                simcard_result['franchise_id'],  # ID da franquia
                rule['variable3_rule'],  # Mensalidade
                rule['created_at_rule'],  # Data de criação
                datetime.now(),  # Data de atualização
                simcard_result['type'],
                vendor_id  # ID do vendedor
            ))
            auto_id += 1
            row_count_sc_rules += 1

    postgres_conn.commit()

    print(f"Total de regras de Simcard inseridas: {row_count_sc_rules}")
    print(f"Total de regras de Hardware inseridas: {row_count_hw_rules}")

except mysql.connector.Error as mysql_error:
    print("Erro no MySQL.")
    print(f"Erro: {mysql_error}")
    if rule:
        print(f"ID: {rule.get('id_rule', 'Desconhecido')}")
    print(traceback.format_exc())
    postgres_conn.rollback()

except Exception as e:
    print("Erro inesperado.")
    print(f"Erro: {e}")
    if rule:
        print(f"ID: {rule.get('id_rule', 'Desconhecido')}")
    print(traceback.format_exc())
    postgres_conn.rollback()

finally:
    mysql_cursor.close()
    mysql_conn.close()
    postgres_cursor.close()
    postgres_conn.close()
