from minio import Minio
import pandas as pd
import requests
from tqdm import tqdm
import sqlalchemy as sa
from sqlalchemy import create_engine
import gc
import os
import sys
from pathlib import Path
import time
from datetime import timedelta
from io import BytesIO 
import psycopg2

def main():
    #grab_data()
    #write_data()
    #write_data_minio()
    #create_tables()
    insert_data()
    



def grab_data() -> None:
    
    print("[*] Get data from -> https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-X.parquet")
    print(f"[*] Destination directory - > {str(os.getcwd())}/data/raw/\n")
    
    #years = ['2018', '2019', '2020', '2021', '2022', '2023']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    year = '2023'
    
    download = False
    
    for month in months:
        parquet_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
        destination_dir= f"./data/raw/yellow_tripdata_{year}-{month}.parquet"

        if os.path.exists(destination_dir):
            continue
        
        try : 
            response = requests.get(parquet_url, stream=True)
            total_size = int(response.headers.get('content-length', 0))

            if response.status_code == 200 : 
                download = True
                with open(destination_dir, 'wb') as file, tqdm(
                    desc=destination_dir,
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
                ) as bar:
                    for data in response.iter_content(chunk_size=1024):
                        size = file.write(data)
                        bar.update(size)   
            
            continue
            
        except requests.exceptions.HTTPError as errh:
            print(f"[Error]: {errh}")
        except requests.exceptions.ConnectionError as errc:
            print(f"[Error]: {errc}")
        except requests.exceptions.Timeout as errt:
            print(f"[Error]: {errt}")
        except requests.exceptions.RequestException as err:
            print(f"[Error]: {err}")
            
        print('\n')
    print("[+] Nothing to do, dataset is up to date") if not download else print("[+] Dataset has been updated")
    print("[+] SUCCESS\n")



def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe
        
def write_data() -> None : 
    
    folder_path = Path("./data/raw/")
    parquet_files = [
        f for f in folder_path.iterdir()
        if f.suffix.lower() == '.parquet' and f.is_file()
    ]
    
    print(f"[*] Processing write data to DB")
    
    #Write data from Minio To PostgreSQL
    #I keep the method just above (the one which consists of writing files stored locally)
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "data"
    objects = client.list_objects(bucket, recursive=True)

    for obj in objects:
        try:
            data = client.get_object(bucket, obj.object_name)
            content = data.read()
            parquet_df = pd.read_parquet(BytesIO(content), engine='pyarrow')
            clean_column_name(parquet_df)
            if not write_data_postgres(parquet_df):
                print(f"[Error] processing {obj.object_name}")
        except Exception as err:
            print(err)
    """
    
    
    total = 0
    start_time = time.time()
    nbr_parquet = len(parquet_files)
    count = 1 
    for parquet_file in parquet_files:
        
        parquet_df = pd.read_parquet(parquet_file, engine='pyarrow')
        total += len(parquet_df)
        clean_column_name(parquet_df)
        
        print(f'[+] Processing parquet file N°:{count}/{nbr_parquet}')
        print(f"[+] {parquet_file.name} ->  Number of rows: {len(parquet_df)}")
        print(f'[+] Size -> {sys.getsizeof(parquet_df) / (1024.0 ** 2):.2f} Mo')
        
        chunk_size = 20000
        num_rows = len(parquet_df)
        
        for i in tqdm(range(0, num_rows, chunk_size), desc=f'Processing {parquet_file.name}', leave=False):
            chunk = parquet_df.iloc[i:i + chunk_size]
            
            if not write_data_postgres(chunk):
                print(f"[Error] processing {parquet_file}")
                
            del chunk
            gc.collect()
        
        del parquet_df 
        gc.collect()
        print('[+] SUCCESS')
        count += 1
        print('\n')

    print(f"[+] SUCCESS -> Total rows : {total}. Uploaded in {str(timedelta(seconds=(time.time() - start_time))).split('.')[0]}")
    
def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "5432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')

    except Exception as e:
        print(f"[Error] connection to the database: {e}")
        return False

    return True

def write_data_minio():
   
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "data"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        
    else:
        print("Bucket " + bucket + " Already Exists")

    local_directory = f'{str(os.getcwd())}/data/raw/'

    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            local_path = os.path.join(root, filename)
            object_name = os.path.relpath(local_path, local_directory)

            client.fput_object(bucket, object_name, local_path)
            print(f"File {object_name} added to bucket {bucket}")

def create_tables() -> None : 
    
    try : 
        print("[*] Connect to DB (nyc_datamart)")
        conn = psycopg2.connect(
            dbname="nyc_datamart",
            user="postgres",
            password="admin",
            host="localhost",
            port="5432"
        )
    except Exception as e : 
        print(f"[*] Error : connection to DB {e}")

    cur = conn.cursor()

    with open("./src/data/create.sql", "r") as file:
        sql_script = file.read()

    cur.execute(sql_script)

    conn.commit()

    cur.close()
    conn.close()
    print("[*] Table created successfully into nyc_datamart.")

def insert_data():
    try:
        conn_target = psycopg2.connect(
            dbname="nyc_datamart",
            user="postgres",
            password="admin",
            host="localhost",
            port="5432"
        )
        print("[*] Connected to DB (nyc_datamart)")

        cur_target = conn_target.cursor()

        with open("./src/data/insert.sql", "r") as file:
            sql_script_target = file.read()


        print("[*] Insert Data into nyc_datamart ...") 
        cur_target.execute(sql_script_target)
        conn_target.commit()
        print("[*] Data inserted successfully into nyc_datamart.")
    except Exception as e:
        print(f"[*] Error: {e}")

    finally:
        if cur_target:
            cur_target.close()
        if conn_target:
            conn_target.close()
if __name__ == '__main__':
    sys.exit(main())
