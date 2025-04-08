import json
import pyodbc
import pandas as pd
import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def main():
    logging.info("Memulai proses ekstraksi data...")
    
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        logging.info("Konfigurasi berhasil dimuat")
    except Exception as e:
        logging.error(f"Gagal membaca config.json: {str(e)}")
        return

    all_data = []
    
    for db_idx, (database, tables_in_db) in enumerate(zip(config['databases'], config['tables'])):
        logging.info(f"Memproses database: {database} ({db_idx+1}/{len(config['databases'])})")
        
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={config['server']};"
            f"DATABASE={database};"
            f"UID={config['username']};"
            f"PWD={config['password']}"
        )
        
        try:
            with pyodbc.connect(connection_string) as conn:
                cursor = conn.cursor()
                process_tables(cursor, database, tables_in_db, config['real_server'], all_data)
        except pyodbc.Error as e:
            logging.error(f"Koneksi gagal ke database {database}: {str(e)}")
    
    save_to_excel(all_data, config['output_file_pattern'])
    logging.info("Proses ekstraksi data selesai")

def process_tables(cursor, database, tables, real_server, all_data):
    for tbl_idx, table_name in enumerate(tables):
        logging.info(f"  Memproses tabel: {table_name} ({tbl_idx+1}/{len(tables)}) di database {database}")
        
        try:
            cursor.execute(f"EXEC sp_help '{table_name}';")
            result_sets = fetch_all_result_sets(cursor)
            
            sample_data = get_sample_data(cursor, table_name)
            
            if len(result_sets) > 1:
                process_columns(result_sets[1], database, table_name, real_server, sample_data, all_data)
        except pyodbc.Error as e:
            logging.error(f"  Gagal memproses tabel {table_name}: {str(e)}")

def fetch_all_result_sets(cursor):
    result_sets = []
    while True:
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            result_sets.append([dict(zip(columns, row)) for row in rows])
        if not cursor.nextset():
            break
    return result_sets

def get_sample_data(cursor, table_name):
    try:
        cursor.execute(f"SELECT TOP 1 * FROM {table_name};")
        sample_row = cursor.fetchone()
        return dict(zip([col[0] for col in cursor.description], sample_row)) if sample_row else {}
    except:
        return {}

def process_columns(columns_info, database, table_name, real_server, sample_data, all_data):
    header_added = False
    for col in columns_info:
        column_name = col.get('Column_name', '')
        data_type = format_data_type(col)
        example = get_example_value(column_name, sample_data)
        
        all_data.append({
            'Server': real_server if not header_added else '',
            'Database': database if not header_added else '',
            'Table': table_name if not header_added else '',
            'Nama_Kolom': column_name,
            'Tipe_Data': data_type,
            '1': '', '2': '', '3': '', '4': '', '5': '',
            'Contoh_Value': example
        })
        header_added = True

def format_data_type(col):
    data_type = col.get('Type', '')
    length = col.get('Length', '')
    if data_type.lower() in ['varchar', 'nvarchar', 'char', 'nchar']:
        return f"{data_type}({length})"
    return data_type

def get_example_value(column_name, sample_data):
    example = sample_data.get(column_name, 'NULL') if sample_data else 'NO DATA'
    if example is None:
        return 'NULL'
    return example if isinstance(example, str) else str(example)

def save_to_excel(data, output_pattern):
    try:
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = output_pattern.format(timestamp=timestamp)
        
        columns = [
            'Server', 'Database', 'Table', 'Nama_Kolom', 
            'Tipe_Data', '1', '2', '3', '4', '5', 'Contoh_Value'
        ]
        
        df = pd.DataFrame(data, columns=columns)
        df.to_excel(output_file, index=False, header=False)
        logging.info(f"Data berhasil disimpan ke {output_file}")
    except Exception as e:
        logging.error(f"Gagal menyimpan file Excel: {str(e)}")

main()