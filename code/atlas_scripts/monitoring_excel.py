import os
import pandas as pd
from pyhive import hive
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from time import sleep
import argparse
from datetime import datetime
import glob
from unidecode import unidecode


# Define the Hive operations
class HiveOps:

    def clean_value(value):
        if pd.isnull(value):
            return 'null'
        elif isinstance(value, str):
            cleaned_value = unidecode(value)
            cleaned_value = cleaned_value.replace("'", "")
            return f"'{cleaned_value}'"
        elif isinstance(value, datetime):
            return f"'{value}'"
        else:
            return str(value)
        

    def create_hive_table(table_name, schema):
        conn = hive.Connection(host="20.55.29.55", port=10000)
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        existing_tables = cursor.fetchall()
        if existing_tables:
            print(f"------------Table '{table_name}' already exists in the database------------")
            return
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for column, data_type in schema.items():
            create_table_query += f"`{column.replace(' ', '_').replace('é', 'e').replace('à', 'a').replace('ç', 'c')}` {data_type}, "
        create_table_query = create_table_query[:-2]  
        create_table_query += f")"

        cursor.execute(create_table_query)
        conn.commit()
        conn.close()
        print(f"------------Table '{table_name}' created successfully------------")
        hive_schema = HiveOps.get_hive_table_schema(table_name)
        print(f"\n--->Hive Table Schema for {table_name}:")
        for column, data_type in hive_schema.items():
            print(f"{column}: {data_type}")
        print("-----------------------------------")

    def insert_data_into_hive_table(table_name, data):
        print(data.dtypes.to_dict())
        conn = hive.Connection(host="20.55.29.55", port=10000)
        cursor = conn.cursor()
        insert_query = f"INSERT INTO TABLE {table_name} VALUES "
        for index, row in data.iterrows():
            values = ','.join([HiveOps.clean_value(value) for value in row.values])
            insert_query += f"({values}),"
        insert_query = insert_query[:-1]
        print(insert_query)
        cursor.execute(insert_query)
        conn.commit()
        conn.close()


    def get_hive_table_schema(table_name):
        conn = hive.Connection(host="20.55.29.55", port=10000)
        cursor = conn.cursor()

        cursor.execute(f"DESCRIBE {table_name}")
        table_schema = {}
        for row in cursor.fetchall():
            column_name = row[0]
            data_type = row[1]
            table_schema[column_name] = data_type

        conn.close()

        return table_schema


    def infer_column_types(df):
        for col in df.columns:
            if df[col].dtype == 'int64':
                continue    
            try:
                df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
            except:
                continue
        print(df.dtypes.to_dict())
        column_types = {}
        for column in df.columns:
            for value in df[column]:
                if isinstance(value, int):
                    column_types[column] = 'BIGINT'
                    break
                elif isinstance(value, float):
                    column_types[column] = 'DOUBLE'
                    break
                elif isinstance(value, datetime):
                    column_types[column] = 'DATE'
                    break
                elif pd.api.types.is_string_dtype(df[column]):
                    column_types[column] = 'STRING'
                    break
        return column_types




def csv_to_hive(csv_file):
    df = pd.read_csv(csv_file)
    df = df.apply(lambda x: x if x.dtype != 'object' else x.apply(lambda y: str(y).replace('\'', ' ').replace('l\'', 'l ')))

    # Infer column types dynamically
    column_types = HiveOps.infer_column_types(df)

    csv_file_name = os.path.basename(csv_file).replace(' ', '_').replace('é', 'e').replace('à', 'a').replace('ç', 'c')
    table_name = os.path.splitext(csv_file_name)[0]
    
    HiveOps.create_hive_table(table_name, column_types)
    HiveOps.insert_data_into_hive_table(table_name, df)
    hive_schema = HiveOps.get_hive_table_schema(table_name)
    print(f"\n--->Hive Table Schema for {table_name}:")
    for column, data_type in hive_schema.items():
        print(f"{column}: {data_type}")
    print("-----------------------------------")
    print("Table is created successfully in the Hive database.")

# Watchdog setup
class ExcelFileHandler(FileSystemEventHandler):
    def __init__(self, hive_ops):
        self.hive_ops = hive_ops

    def on_created(self, event):
        if not event.src_path.endswith('.xlsx'):
            return
        print(f"Detected new Excel file: {event.src_path}")
        self.hive_ops.excel_to_hive(event.src_path)

def process_new_excel_files(**kwargs):
    path_to_watch = 'C:/Users/Maadoudi/Desktop/new_data'  # Directory to check for Excel files
    excel_files = glob.glob(os.path.join(path_to_watch, '*.csv'))
    
    for excel_file in excel_files:
        print(f"Processing file: {excel_file}")
        csv_to_hive(excel_file)
        # Optionally, move or delete the file after processing to prevent reprocessing
        # os.remove(excel_file)
process_new_excel_files()
# def start_monitoring(path_to_watch):
#     hive_ops = HiveOps()
#     event_handler = ExcelFileHandler(hive_ops)
#     observer = Observer()
#     observer.schedule(event_handler, path_to_watch, recursive=False)
#     observer.start()
#     print(f"Monitoring {path_to_watch} for new Excel files...")
#     try:
#         while True:
#             sleep(1)
#     except KeyboardInterrupt:
#         observer.stop()
#     observer.join()

# if __name__ == "__main__":
#     path_to_watch = 'C:/Users/Maadoudi/Desktop/PFE/Data'  # Modify this to the path you want to monitor
#     start_monitoring(path_to_watch)

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description='Monitor a directory for new Excel files and process them with Hive.')
#     parser.add_argument('path_to_watch', type=str, help='The path to the directory you want to monitor')
#     args = parser.parse_args()

#     start_monitoring(args.path_to_watch)