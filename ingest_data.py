
import pandas as pd
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine
import psycopg2
import os
import argparse #allows us to pass commandline arguments. 

# def main(params):
#     user = params.user
#     password = params.password
#     host = params.host
#     port = params.port
#     db = params.db
#     table_name = params.table_name
#     url = params.url #"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
#     parquet_name = 'output.parquet'
#     csv_name = 'output.csv'
    

#     #download parquet file
#     os.system(f"wget {url} -O {parquet_name}")

#     #We want to put the schema into Postgres
#     engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
#     engine.connect()
    
#     #Save the schema to Postgres
    
#     #print(pd.io.sql.get_schema(df2, name='yellow_taxi_data',con=engine))
    
#     #Keep in mind that (pd.io.sql.get_schema) is used to generate a CREATE TABLE statement based on the
#     # #DataFrame's structure. If you want to actually create the table in a database using the generated schema,
#     #you would typically execute the generated SQL statement.


#     # Read the first chunk to get the column names
#     #df = pd.read_csv('yellow_tripdata_2021-01', nrows=1)

#     # Exclude 'Unnamed: 0' from the columns
#     #columns_to_use = [col for col in df.columns if col != 'Unnamed: 0']

#     # Read the Parquet file
#     table = pq.read_table(parquet_name)
#     # Convert to Pandas DataFrame
#     df = table.to_pandas()
#     # Save as CSV
#     df.to_csv(csv_name, index=False)

#     #Breaking data into chuncks
#     df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

#     df = next(df_iter)

#     df.head(n=0).to_sql(name=table_name,con=engine, if_exists='replace')


#     df.to_sql(name=table_name,con=engine, if_exists='append')


#     while True:
#         t_start = time()
#         df = next(df_iter)
#         df.to_sql(name=table_name,con=engine, if_exists='append')
#         t_end =time()
#         print('inserted another chunk, took %.3f seconds' %(t_end-t_start))



# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description="Ingest PARQUET data to Postgres")
#     parser.add_argument('--user',help='user name for postgres')
#     parser.add_argument('--password',help='password for postgres')
#     parser.add_argument('--host',help='host for postgres')
#     parser.add_argument('--port',help='port for postgres')
#     parser.add_argument('--db',help='database name for postgres')
#     parser.add_argument('--table_name',help='name of the table where we will write the results to')
#     parser.add_argument('--url',help='url of the csv file')
    
#     #parse the arguments
#     args = parser.parse_args()

#     main(args)

#df = pd.read_parquet('yellow_tripdata_2021-01.parquet')

#df1 = pd.read_csv('yellow_tripdata_2021-01')

#getting the Schema
#print(pd.io.sql.get_schema(df2, name='yellow_taxi_data'))


user = "root"
password = "root"
host = "pgdatabase"
port = "5432"
db ="ny_taxi"
table_name = "yellow_taxi_data"
table_Zones = "zones_data"
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
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