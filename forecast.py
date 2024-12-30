from datetime import datetime
import random
import time
import traceback
import uuid
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

try:
    print("STARTING SCRIPT", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    mysql_cursor.execute("""
    SELECT * FROM forecast;
    """)
    forecasts = mysql_cursor.fetchall()

    row_count = 0
    auto_id = 1

    for forecast in forecasts:
        try:
            postgres_cursor.execute("""
            SELECT id FROM users WHERE username = %s
            """, (forecast['vendor_forecast'],))
            user_result = postgres_cursor.fetchone()

            null_uuid = str(uuid.UUID("c531f1e9-b8b8-40e8-8efa-5bed8cdaae64"))
            vendor_id = user_result['id'] if user_result else null_uuid

            postgres_cursor.execute("""
                INSERT INTO crm_forecast (id, cliente, vendedor, type_forecast, mes_referencia, ano_referencia, marca, modelo, quantidade, validade, created_at, updated_at)
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """, (
                auto_id,
                forecast.get('client_forecast'),
                vendor_id,
                forecast.get('type_forecast'),
                forecast.get('month_ref_forecast'),
                forecast.get('year_ref_forecast'),
                forecast.get('brand_forecast'),
                forecast.get('model_forecast'),
                forecast.get('quantity_forecast'),
                forecast.get('validity_forecast'),
                forecast.get('created_at_forecast'),
                forecast.get('last_update_forecast'),

            ))

            auto_id += 1
            row_count += 1

        except Exception as e:
            postgres_conn.rollback()
            print(f"Erro no pedido ID: {forecast['id_forecast']}")
            print(traceback.format_exc())

    postgres_conn.commit()

    print(f"Total de linhas inseridas: {row_count}")
    print("END SCRIPT", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
except Exception as e:
    postgres_conn.rollback()
    print("Erro geral:")
    print(traceback.format_exc())
