import traceback
import uuid
from bs4 import BeautifulSoup
import mysql.connector
import psycopg2
from psycopg2.extras import DictCursor, execute_values
from dotenv import load_dotenv
import os
from datetime import datetime

# Carregar variáveis de ambiente
load_dotenv()

# Configuração das conexões com os bancos de dados
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

# Funções auxiliares


def clean_numeric(value):
    if isinstance(value, str):
        value = value.replace('.', '')  # Remove separador de milhares
        value = value.replace(',', '.')  # Converte separador decimal
        try:
            return float(value)
        except ValueError:
            return None
    return value



def convert_to_boolean(value):
    if isinstance(value, str):
        value = value.strip().lower()
        if value in ['sim', 'true', 'yes']:
            return True
        elif value in ['não', 'nao', 'false', 'no', 'não possui']:
            return False
    return False


def extract_td_values(html_content):
    if isinstance(html_content, str):
        soup = BeautifulSoup(html_content, "html.parser")
        return [td.get_text().strip() for td in soup.find_all("td")]
    return []


def extract_table_values(html_content):
    if isinstance(html_content, str):
        soup = BeautifulSoup(html_content, "html.parser")
        # Extrai os textos diretamente das tabelas
        return [table.get_text().strip() for table in soup.find_all("table")]
    return []


def clean_date(value):
    if isinstance(value, str) and value.strip():
        try:
            return datetime.strptime(value, "%Y/%m/%d").strftime("%Y-%m-%d")
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
            except ValueError:
                return None
    return None


approval_status_mapping = {
    "Pendente": 1,
    "Recusado": 2,
    "Aprovado": 3
}

# Conexões com os bancos
mysql_conn = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor(dictionary=True)

postgres_conn = psycopg2.connect(**postgres_config)
postgres_cursor = postgres_conn.cursor(cursor_factory=DictCursor)

try:
    print("STARTING SCRIPT", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # Consultar os pedidos no MySQL
    mysql_cursor.execute("""
    SELECT * FROM orders WHERE type_order = 'Hardware'
    """)
    orders = mysql_cursor.fetchall()

    row_count = 0

    for order in orders:
        try:
            postgres_cursor.execute("""
                SELECT id FROM users WHERE username = %s
            """, (order['vendor_order'],))
            user_result = postgres_cursor.fetchone()
            null_uuid = str(uuid.UUID("c531f1e9-b8b8-40e8-8efa-5bed8cdaae64"))
            vendor_id = user_result['id'] if user_result else null_uuid

            postgres_cursor.execute("""
                SELECT id, family FROM crm_hardwares WHERE brand = %s AND model = %s AND sell_type = %s;
            """, (order['brand_order'], order['model_order'], order['type_sale_order']))
            hardware_id = postgres_cursor.fetchone()

            postgres_cursor.execute("""
                SELECT id FROM crm_status_pedido_hardware WHERE name = %s
            """, (order['status_order'],))
            status_result = postgres_cursor.fetchone()
            status_id = status_result['id'] if status_result else None

            promocao = convert_to_boolean(order['promo_order'])
            simcard_allcom = convert_to_boolean(order['sc_allcom_order'])
            config_order = convert_to_boolean(order['config_order'])
            entrada = convert_to_boolean(order['variable1_order'])
            isentar_frete = convert_to_boolean(order['shipping_freight_exemption'])
            allcom_approval_status = approval_status_mapping.get(order['aprove_allcom_order'], 1)
            financeiro_approval_status = approval_status_mapping.get(order['aprove_financial_order'], 1)

            iccid = extract_td_values(order['iccid_order'])
            msisdn = extract_td_values(order['callerid_order'])
            imei = extract_table_values(order['imei_order'])
            price_order = clean_numeric(order['price_order'])

            created_at_order = clean_date(order['created_at_order'])
            shipping_date_order = clean_date(order['shipping_date_order'])


            postgres_cursor.execute("""
            INSERT INTO crm_pedidos_hardware (
                id, cliente, responsible_seller_id, id_hardware, marca, modelo, family, sell_type,
                quantidade, moeda, preco, entrada, prazo_pagamento, comissao, simcard_allcom,
                configuracao, promocao, status, fase_aprovacao, fase_aprovacao_financeiro, forma_de_envio, insentar_frete,
                pais, cep_zipcode, estado, cidade, rua, numero, complemento, observacoes, created_at, data_envio, iccid, msisdn, imei
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """, (
                order.get('id_order'),
                order.get('client_order'),
                vendor_id,
                hardware_id.get('id'),
                order.get('brand_order'),
                order.get('model_order'),
                hardware_id.get('family'),
                order.get('type_sale_order'),
                order.get('quantity_order'),
                order.get('coin_order'),
                price_order, 
                entrada,
                order.get('payment_order'),
                order.get('comission_order'),
                simcard_allcom,
                config_order,
                promocao, 
                status_id,
                allcom_approval_status,
                financeiro_approval_status,
                order.get('shipping_order'),
                isentar_frete,
                order.get('country_order'),
                order.get('cep_order'),
                order.get('uf_order'),
                order.get('city_order'),
                order.get('address_order'),
                order.get('number_order'),
                order.get('complement_order'),
                order.get('obs_order'),
                created_at_order,
                shipping_date_order,
                iccid,
                msisdn,
                imei
            ))

        except Exception as e:
            postgres_conn.rollback()
            print(f"Erro no pedido ID: {order['id_order']}")
            print(f"Status: {order['status_order']}")
            print(traceback.format_exc())

        postgres_conn.commit()
        row_count += 1

    print(f"Total de linhas inseridas: {row_count}")
    print("END SCRIPT", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
except Exception as e:
    print("Erro geral:")
    print(traceback.format_exc())
    postgres_conn.rollback()

finally:
    mysql_cursor.close()
    mysql_conn.close()
    postgres_cursor.close()
    postgres_conn.close()
