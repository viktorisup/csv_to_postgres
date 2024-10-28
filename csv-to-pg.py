import pandas as pd
import glob
import os
from sqlalchemy import create_engine

db_user = 'xxxx'
db_password = 'xxxx'
db_host = 'xxxxxx'
db_port = '5432'
db_name = 'xxxxx'
db_table_name = 'xxxxxxxx'

connection_string = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(connection_string)

path = "./"
all_files = sorted(glob.glob(os.path.join(path, "*.csv")))

#Загрузка CSV-файлов в таблицу
for filename in all_files:
    print(f"Загрузка файла: {filename}")
    try:
        # Чтение CSV-файла
        data = pd.read_csv(filename, low_memory=False)
        data['eu_country_card'] = data['eu_country_card'].astype(bool)
        data['decline_commission'] = data['decline_commission'].astype(bool)

        # Загрузка данных в таблицу
        data.to_sql(db_table_name, con=engine, index=False, if_exists='append')

        print(f"Файл {filename} успешно загружен.")
    except Exception as e:
        print(f"Ошибка при загрузке файла {filename}: {e}")

print("Загрузка всех файлов завершена.")
