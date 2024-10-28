import pandas as pd
from sqlalchemy import create_engine
from concurrent.futures import ProcessPoolExecutor
import os
import glob


def process_file(filename):
    db_user = 'xxxxxx'
    db_password = 'xxxxx'
    db_host = 'xxx.xxx.xxx.xxx'
    db_port = '5432'
    db_name = 'xxxxxx'
    db_table_name = 'xxxxxxxx'
    connection_string = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    try:
        data = pd.read_csv(filename, low_memory=False)
        data['eu_country_card'] = data['eu_country_card'].astype(bool)
        data['decline_commission'] = data['decline_commission'].astype(bool)

        # Создаем подключение к базе данных
        engine = create_engine(connection_string)

        # Запись в базу данных
        data.to_sql(db_table_name, con=engine, index=False, if_exists='append')
        print(f"Файл {filename} успешно загружен.")
    except Exception as e:
        print(f"Ошибка при загрузке файла {filename}: {e}")

if __name__ == '__main__':
    path = './'
    all_files = sorted(glob.glob(os.path.join(path, "*.csv")))

    with ProcessPoolExecutor() as executor:
        executor.map(process_file, all_files)