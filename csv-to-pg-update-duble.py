import gzip
from io import StringIO
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
import psycopg2
import requests
from datetime import datetime, timedelta, timezone
import argparse


parser = argparse.ArgumentParser(description="Arguments")
parser.add_argument('--init', action='store_true', help="Create/Recreate DB table")
parser.add_argument('-g', type=int, nargs='+', default=None, help="Gateway ID")
parser.add_argument('-c', type=int, default=3600*24*3, help="Export with txn_created_at last N seconds (default: 3 days)")
parser.add_argument('-u', type=int, default=90, help="Export with txn_updated_at last N seconds (default: 90 sec)")
args = parser.parse_args()

PSP_EXPORT_1C_BASE_URL = "https://xxxxx/api/txn"

db_user = 'xxxxx'
db_password = 'xxxxxxxx'
db_host = 'xxxxxxx'
db_port = '5432'
db_name = 'xxxxxx'
db_table_name = 'transactions_info'
svc_table_name = 'svc_table'
conn_string_pandas = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
conn_string_pg = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

init_query = f"""
DROP TABLE IF EXISTS {db_table_name};
CREATE TABLE {db_table_name} (
    txn_id VARCHAR PRIMARY KEY,
    project_id INTEGER,
    mst_id VARCHAR,
    merch_id VARCHAR,
    txn_amount_src NUMERIC,
    txn_currency_src VARCHAR,
    txn_amount NUMERIC,
    txn_currency VARCHAR,
    txn_type_id VARCHAR,
    pay_method_id VARCHAR,
    txn_status_id VARCHAR,
    txn_error_code VARCHAR,
    chn_id INTEGER,
    gtw_id INTEGER,
    gtw_desc VARCHAR,
    mst_order_id VARCHAR,
    order_id VARCHAR,
    mst_txn_id VARCHAR,
    gtw_txn_id VARCHAR,
    card_first_6 VARCHAR,
    card_last_4 VARCHAR,
    cardinfo_pay_system VARCHAR,
    ip VARCHAR,
    rrn VARCHAR,
    email VARCHAR,
    full_name VARCHAR,
    auth_code VARCHAR,
    ps_error_code VARCHAR,
    ps_error_message VARCHAR,
    cardinfo_issuer_name VARCHAR,
    cardinfo_country VARCHAR,
    card_issuer_country VARCHAR,
    eu_country_card BOOLEAN,
    decline_commission BOOLEAN,
    txn_authorized_at TIMESTAMPTZ,
    txn_confirmed_at  TIMESTAMPTZ,
    txn_reconciled_at TIMESTAMPTZ,
    txn_settled_at    TIMESTAMPTZ,
    txn_updated_at    TIMESTAMPTZ,
    txn_created_at    TIMESTAMPTZ,
    txn_cms_updated_at TIMESTAMPTZ,
    txn_cms_created_at TIMESTAMPTZ
);
"""

svc_init_query = f"""
DROP TABLE IF EXISTS {svc_table_name};
CREATE TABLE {svc_table_name} (
    id SERIAL PRIMARY KEY,
    status BOOLEAN,
    date TIMESTAMP
)
"""


def init_tables(conn_str, *queries):
    try:
        connection = psycopg2.connect(conn_str)
        cursor = connection.cursor()
        try:
            for query in queries:
                cursor.execute(query)
            connection.commit()
        except Exception as e:
            connection.rollback()
            print(f"Ошибка инициализации : {e}")
        finally:
            cursor.close()
            connection.close()
    except Exception as e:
        print(f"Ошибка подключение к базе: {e}")


def get_csv(url, params: dict):
    requests.packages.urllib3.disable_warnings()
    response = requests.get(url, params=params, verify=False)
    final_url = response.url
    print(final_url)
    content = response.content
    decode_content = gzip.decompress(content).decode('utf-8')
    return decode_content


def svc_func(conn_str, table):
    connection = psycopg2.connect(conn_str)
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM {table} ORDER BY date DESC LIMIT 5000")
        values_list = cursor.fetchall()
        index = 0
        count_false = 0
        now = datetime.now(timezone.utc)
        date_from = now - timedelta(seconds=args.c)
        updated_from = now - timedelta(seconds=args.u)
        if not values_list:
            return date_from, updated_from
        elif values_list[0][1] is True:
            difference = now - values_list[0][2].replace(tzinfo=timezone.utc)
            difference_in_seconds = difference.total_seconds()
            if int(difference_in_seconds) > 90:
                date_from = values_list[0][2]
                updated_from = values_list[0][2]
                return date_from, updated_from
            else:
                return date_from, updated_from
        elif values_list[0][1] is False and values_list[1][1] is True:
            date_from = values_list[1][2]
            updated_from = values_list[1][2]
            return date_from, updated_from
        elif values_list[0][1] is False and values_list[1][1] is False:
            for i in range(len(values_list)):
                if values_list[i][1] is False:
                    count_false += 1
                    if count_false >= 2:
                        index = i
                else:
                    break
            date_from = values_list[index + 1][2]
            updated_from = values_list[index + 1][2]
            return date_from, updated_from
    finally:
        cursor.close()
        connection.close()


def svc_record(conn_str, table, status: bool):
    connection = psycopg2.connect(conn_str)
    cursor = connection.cursor()
    try:
        now = datetime.now(timezone.utc)
        dict_tmp = {'status': status, 'date': now}
        cursor.execute(
            f'INSERT INTO {table} (status, date) VALUES(%s,%s)', (dict_tmp['status'], dict_tmp['date']))
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def write_csv(conn_str, table, content):
    engine = create_engine(conn_str)
    try:
        data = StringIO(content)
        data_frame = pd.read_csv(data, low_memory=False)
        data_frame['eu_country_card'] = data_frame['eu_country_card'].astype(bool)
        data_frame['decline_commission'] = data_frame['decline_commission'].astype(bool)
        columns_to_exclude = ['mst_name', 'merch_name', 'order_desc']
        data_frame = data_frame.drop(columns=columns_to_exclude)
        data_frame.to_sql(table, engine, if_exists='append', index=False, method=upsert)
        print('Данные записаны')
        return True
    except Exception as e:
        print(f"Ошибка при записи данных: {e}")
    finally:
        engine.dispose()


def upsert(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['txn_id'],
        set_={col.name: col for col in insert_stmt.excluded}
    )
    conn.execute(update_stmt)

chn_id =[118, 144, 146, 147, 148, 149, 150, 170, 188, 199, 212, 276, 277, 278, 303, 304, 305,
         324, 403, 405, 409, 410, 413, 414, 398, 332, 333, 402, 404, 401, 340, 342, 412, 341, 411, 334, 480, 479, 478, 477]
def main():
    if args.init:
        init_tables(conn_string_pg, init_query, svc_init_query)
        return

    # now = datetime.now(timezone.utc)
    # date_from = now - timedelta(seconds=args.c)
    # updated_from = now - timedelta(seconds=args.u)

    date_from, updated_from = svc_func(conn_string_pg, svc_table_name)
    params = {
        "format": "csv",
        "compression": "gzip",
        "txn_cms_created_at": "any",
        "chn_id": chn_id,
        "date_from": '2024-10-31 00:00:00', #date_from.strftime('%Y-%m-%d %H:%M:%S'),
        "updated_from": '2024-10-31 00:00:00', #updated_from.strftime('%Y-%m-%d %H:%M:%S'),
        # "date_to": '2024-11-05 00:00:00',
        # "updated_to": '2024-11-05 00:00:00'
    }
    data = get_csv(PSP_EXPORT_1C_BASE_URL, params)
    status = False
    try:
        status = write_csv(conn_string_pandas, db_table_name, data)
    finally:
        svc_record(conn_string_pg, svc_table_name, status)
if __name__ == '__main__':
    main()
