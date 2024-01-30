import pandas as pd
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine
import psycopg2
import os
import argparse #allows us to pass commandline arguments. 


user = "root"
password = "root"
host = "pgdatabase"
port = "5432"
db ="ny_taxi"
table_name = "green_taxi_data"
table_Zones = "zones_data"
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-09.parquet"
urlZones = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
parquet_name = 'output.parquet'
csv_name = 'output.csv'
csv_zones = 'zones.csv'
    

# download parquet file
os.system(f"wget {url} -O {parquet_name}")

# download csv file for zones
os.system(f"wget {urlZones} -O {csv_zones}")
#We want to put the schema into Postgres
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
engine.connect()
    
    # Read the Parquet file
table = pq.read_table(parquet_name)
    # Convert to Pandas DataFrame
   
df = table.to_pandas()
    # Save as CSV
df.to_csv(csv_name, index=False)

    #Breaking data into chuncks
df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
df = next(df_iter)
df.head(n=0).to_sql(name=table_name,con=engine, if_exists='replace')
df.to_sql(name=table_name,con=engine, if_exists='append')

df_zones = pd.read_csv(csv_zones)
df_zones.head(n=0).to_sql(name=table_Zones,con=engine, if_exists='replace')
df_zones.to_sql(name=table_Zones, con=engine, if_exists='append')


while True:
    t_start = time()
    df = next(df_iter)
    df.to_sql(name=table_name,con=engine, if_exists='append')
    t_end =time()
    print('inserted another chunk, took %.3f seconds' %(t_end-t_start))


# build:
#       context: . # Path to directory containing Dockerfile for pgadmin service
#       dockerfile: Dockerfile  # Name of Dockerfile for pgadmin service