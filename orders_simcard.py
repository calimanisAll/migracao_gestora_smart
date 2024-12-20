# import traceback
# import uuid
# from bs4 import BeautifulSoup
# import mysql.connector
# import psycopg2
# from psycopg2.extras import DictCursor, execute_values
# from dotenv import load_dotenv
# import os
# from datetime import datetime

# # Carregar variáveis de ambiente
# load_dotenv()

# # Configuração das conexões com os bancos de dados
# mysql_config = {
#     'host': os.getenv('MYSQL_DATABASE_HOST'),
#     'user': os.getenv('MYSQL_DATABASE_USER'),
#     'password': os.getenv('MYSQL_DATABASE_PASSWORD'),
#     'database': os.getenv('MYSQL_DATABASE'),
# }

# postgres_config = {
#     'host': os.getenv('POSTGRES_DATABASE_HOST'),
#     'user': os.getenv('POSTGRES_DATABASE_USER'),
#     'password': os.getenv('POSTGRES_DATABASE_PASSWORD'),
#     'database': os.getenv('POSTGRES_DATABASE'),
# }

# # Funções auxiliares


# def clean_numeric(value):
#     if isinstance(value, str):
#         value = ''.join(filter(lambda x: x.isdigit()
#                         or x in [',', '.'], value))
#         value = value.replace(',', '.')
#         try:
#             return float(value)
#         except ValueError:
#             return None
#     return value


# def convert_to_boolean(value):
#     if isinstance(value, str):
#         value = value.strip().lower()
#         if value in ['sim', 'true', 'yes']:
#             return True
#         elif value in ['não', 'nao', 'false', 'no', 'não possui']:
#             return False
#     return False


# def extract_td_values(html_content):
#     if isinstance(html_content, str):
#         soup = BeautifulSoup(html_content, "html.parser")
#         return [td.get_text().strip() for td in soup.find_all("td")]
#     return []


# def map_variable2_order(value):
#     mapping = {
#         "4FF - Nano": 1,
#         "3FF - Micro": 2,
#         "2FF - Padrão": 3,
#         "Triplo Corte": 4
#     }
#     return mapping.get(value, None)


# def clean_date(value):
#     """
#     Valida e converte um valor para o formato de data esperado.
#     Retorna None se o valor for inválido ou vazio.
#     """
#     if isinstance(value, str) and value.strip():
#         try:
#             # Tente converter para o formato ISO (YYYY-MM-DD)
#             return datetime.strptime(value, "%Y/%m/%d").strftime("%Y-%m-%d")
#         except ValueError:
#             try:
#                 # Tente um formato alternativo, se necessário
#                 return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
#             except ValueError:
#                 return None
#     return None


# operator_mapping = {
#     "Vivo": 1,
#     "Vivo (Pública)": 1,
#     "Claro": 2,
#     "Tim": 3,
#     "Tim (Pública)": 3,
#     "Algar 5 Operadoras": 6,
#     "Vivo Catm1": 11,
#     "Arqia Internacional": 15,
#     "Claro Internacional": 17,
#     "Arqia (TIM + VIVO)": 18,
#     "Vivo BL": 19,
#     "VIVO VOZ": 21,
#     "Claro BL": 22,
#     "Algar 3 Operadoras": 23,
#     "Claro CATM1": 24,
#     "Sierra": 25,
#     "Oi": 26,
#     "OI": 26
# }

# approval_status_mapping = {
#     "Pendente": 1,
#     "Recusado": 2,
#     "Aprovado": 3
# }

# # Conexões com os bancos
# mysql_conn = mysql.connector.connect(**mysql_config)
# mysql_cursor = mysql_conn.cursor(dictionary=True)

# postgres_conn = psycopg2.connect(**postgres_config)
# postgres_cursor = postgres_conn.cursor(cursor_factory=DictCursor)

# try:
#     # Consultar os pedidos no MySQL
#     mysql_cursor.execute("""
#     SELECT
#         id_order, created_at_order, vendor_order, client_order, cnpj_cpf_order, type_order,
#         status_order, brand_order, model_order, price_order, quantity_order, variable1_order,
#         variable2_order, variable3_order, variable4_order, variable5_order, variable6_order,
#         variable7_order, sc_contract_order, shipping_fee_order, type_sale_order, payment_order,
#         shipping_order, shipping_preview_date_order, shipping_date_order, financial_release_date_order,
#         promo_order, coin_order, address_order, number_order, complement_order, city_order, uf_order,
#         cep_order, country_order, obs_order, tracking_number_order, sc_allcom_order, config_order,
#         config_hw_order, callerid_order, iccid_order, iccid_subst_order, imei_order, comission_order,
#         nf_order, aprove_allcom_order, date_aprove_allcom_order, user_aprove_allcom_order,
#         aprove_financial_order, date_aprove_financial_order, user_aprove_financial_order,
#         aprove_client_order, aprove_client_sent_email_order, aprove_client_sent_date_order,
#         aprove_client_content_email_order, date_aprove_client_order, name_aprove_client_order,
#         email_aprove_client_order, rg_aprove_client_order, cpf_aprove_client_order, ip_aprove_client_order,
#         user_last_update, updated_at_order, cod_service_shipping, value_service_shipping,
#         shipping_freight_exemption, date_invoicing_omie
#     FROM orders WHERE type_order = 'Simcard'
#     """)
#     orders = mysql_cursor.fetchall()

#     row_count = 0

#     for order in orders:
#         try:
#             # Obter ID do vendedor no PostgreSQL
#             postgres_cursor.execute("""
#                 SELECT id FROM users WHERE username = %s
#             """, (order['vendor_order'],))
#             user_result = postgres_cursor.fetchone()
#             null_uuid = str(uuid.UUID("c531f1e9-b8b8-40e8-8efa-5bed8cdaae64"))
#             vendor_id = user_result['id'] if user_result else null_uuid

#             postgres_cursor.execute("""
#                 SELECT id FROM crm_status_pedido_simcard WHERE name = %s
#             """, (order['status_order'],))
#             status_result = postgres_cursor.fetchone()
#             status_id = status_result['id']

#             operator_id = operator_mapping.get(order['brand_order'], None)

#             franchise_value, franchise_type = None, None
#             if order['model_order']:
#                 value_part = ""
#                 type_part = ""
#                 found_alpha = False

#                 for char in order['model_order']:
#                     if char.isdigit() or char in "MBkbGB":
#                         if not found_alpha:
#                             value_part += char
#                         else:
#                             type_part += char
#                     else:
#                         found_alpha = True
#                         type_part += char

#                 franchise_value = value_part.strip()
#                 franchise_type = type_part.strip() if type_part else None

#             postgres_cursor.execute("""
#                 SELECT id FROM crm_franchises
#                 WHERE franchise = %s AND type = %s
#             """, (franchise_value, franchise_type))
#             franchise_result = postgres_cursor.fetchone()

#             if not franchise_result:
#                 raise ValueError(f"Franquia não encontrada para o valor: {
#                                  franchise_value} e tipo: {franchise_type}")

#             franchise_id = franchise_result['id']

#             postgres_cursor.execute("""
#                 SELECT id FROM crm_contracts 
#                 WHERE operadora = %s AND franquia = %s
#             """, (operator_id, franchise_id))
#             contract_result = postgres_cursor.fetchone()

#             promocao = convert_to_boolean(order['promo_order'])
#             isentar_frete = convert_to_boolean(
#                 order['shipping_freight_exemption'])
#             envio_sms = convert_to_boolean(order['variable3_order'])
#             variable2_order = map_variable2_order(order['variable2_order'])
#             allcom_approval_status = approval_status_mapping.get(
#                 order['aprove_allcom_order'], 1)
#             contract_id = contract_result['id'] if contract_result else None

#             if variable2_order is None:
#                 raise ValueError(f"Valor de variable2_order inválido: {
#                                  order['variable2_order']}")

#             iccid = extract_td_values(order['iccid_order'])
#             msisdn = extract_td_values(order['callerid_order'])
#             price_order = clean_numeric(order['price_order'])
#             ativacao = clean_numeric(order['variable1_order'])

#             # Limpeza dos campos de data
#             created_at_order = clean_date(order['created_at_order'])
#             shipping_date_order = clean_date(order['shipping_date_order'])

#             postgres_cursor.execute("""
#                 INSERT INTO crm_pedidos_simcard
#                 (
#                     id, cliente, responsible_seller_id, id_contrato, operadora, franquia, moeda, mensalidade,
#                     quantidade, ativacao, pre_ativacao, trade_in, envio_sms, recorrencia, corte_simcard, 
#                     promocao, forma_de_envio, insentar_frete, status, fase_aprovacao, pais, cep_zipcode, 
#                     estado, cidade, rua, numero, complemento, observacoes, created_at, data_envio, iccid, msisdn
#                 )
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
#                         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             """, (
#                 order['id_order'], order['client_order'], vendor_id, contract_id,
#                 operator_id, franchise_id, order['coin_order'], price_order,
#                 order['quantity_order'], ativacao, order['variable4_order'],
#                 order['payment_order'], envio_sms, order['type_sale_order'],
#                 variable2_order, promocao, order['shipping_order'], isentar_frete,
#                 status_id, allcom_approval_status, order['country_order'],
#                 order['cep_order'], order['uf_order'], order['city_order'], order['address_order'],
#                 order['number_order'], order['complement_order'], order['obs_order'],
#                 created_at_order, shipping_date_order, iccid, msisdn
#             ))

#         except Exception as e:
#             postgres_conn.rollback()
#             print(f"Erro no pedido ID: {order['id_order']}")
#             print(f"Status: {order['status_order']}")
#             print(traceback.format_exc())
        
#         postgres_conn.commit()
#         row_count += 1
        
#     print(f"Total de linhas inseridas: {row_count}")

# except Exception as e:
#     print("Erro geral:")
#     print(traceback.format_exc())
#     postgres_conn.rollback()

# finally:
#     mysql_cursor.close()
#     mysql_conn.close()
#     postgres_cursor.close()
#     postgres_conn.close()


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
}

# Funções auxiliares
def clean_numeric(value):
    if isinstance(value, str):
        value = ''.join(filter(lambda x: x.isdigit() or x in [',', '.'], value))
        value = value.replace(',', '.')
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

def map_variable2_order(value):
    mapping = {
        "4FF - Nano": 1,
        "3FF - Micro": 2,
        "2FF - Padrão": 3,
        "Triplo Corte": 4
    }
    return mapping.get(value, None)

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
    "OI": 26
}

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
    # Consultar os pedidos no MySQL
    mysql_cursor.execute("""
    SELECT * FROM orders WHERE type_order = 'Simcard'
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
                SELECT id FROM crm_status_pedido_simcard WHERE name = %s
            """, (order['status_order'],))
            status_result = postgres_cursor.fetchone()
            status_id = status_result['id'] if status_result else None

            operator_id = operator_mapping.get(order['brand_order'], None)

            franchise_value, franchise_type = None, None
            if order['model_order']:
                value_part, type_part = "", ""
                found_alpha = False

                for char in order['model_order']:
                    if char.isdigit() or char in "MBkbGB":
                        if not found_alpha:
                            value_part += char
                        else:
                            type_part += char
                    else:
                        found_alpha = True
                        type_part += char

                franchise_value = value_part.strip()
                franchise_type = type_part.strip() if type_part else None

            postgres_cursor.execute("""
                SELECT id FROM crm_franchises
                WHERE franchise = %s AND type = %s
            """, (franchise_value, franchise_type))
            franchise_result = postgres_cursor.fetchone()

            franchise_id = franchise_result['id'] if franchise_result else None

            postgres_cursor.execute("""
                SELECT id FROM crm_contracts
                WHERE operadora = %s AND franquia = %s
            """, (operator_id, franchise_id))
            contract_result = postgres_cursor.fetchone()

            contract_id = contract_result['id'] if contract_result else None

            promocao = convert_to_boolean(order['promo_order'])
            isentar_frete = convert_to_boolean(order['shipping_freight_exemption'])
            envio_sms = convert_to_boolean(order['variable3_order'])
            variable2_order = map_variable2_order(order['variable2_order'])
            allcom_approval_status = approval_status_mapping.get(order['aprove_allcom_order'], 1)

            iccid = extract_td_values(order['iccid_order'])
            msisdn = extract_td_values(order['callerid_order'])
            price_order = clean_numeric(order['price_order'])
            ativacao = clean_numeric(order['variable1_order'])

            created_at_order = clean_date(order['created_at_order'])
            shipping_date_order = clean_date(order['shipping_date_order'])

            postgres_cursor.execute("""
                INSERT INTO crm_pedidos_simcard
                (
                    id, cliente, responsible_seller_id, id_contrato, operadora, franquia, moeda, mensalidade,
                    quantidade, ativacao, pre_ativacao, trade_in, envio_sms, recorrencia, corte_simcard,
                    promocao, forma_de_envio, insentar_frete, status, fase_aprovacao, pais, cep_zipcode,
                    estado, cidade, rua, numero, complemento, observacoes, created_at, data_envio, iccid, msisdn
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order['id_order'], order['client_order'], vendor_id, contract_id,
                operator_id, franchise_id, order['coin_order'], price_order,
                order['quantity_order'], ativacao, order['variable4_order'],
                order['payment_order'], envio_sms, order['type_sale_order'],
                variable2_order, promocao, order['shipping_order'], isentar_frete,
                status_id, allcom_approval_status, order['country_order'],
                order['cep_order'], order['uf_order'], order['city_order'], order['address_order'],
                order['number_order'], order['complement_order'], order['obs_order'],
                created_at_order, shipping_date_order, iccid, msisdn
            ))

        except Exception as e:
            postgres_conn.rollback()
            print(f"Erro no pedido ID: {order['id_order']}")
            print(f"Status: {order['status_order']}")
            print(traceback.format_exc())

        postgres_conn.commit()
        row_count += 1

    print(f"Total de linhas inseridas: {row_count}")

except Exception as e:
    print("Erro geral:")
    print(traceback.format_exc())
    postgres_conn.rollback()

finally:
    mysql_cursor.close()
    mysql_conn.close()
    postgres_cursor.close()
    postgres_conn.close()
