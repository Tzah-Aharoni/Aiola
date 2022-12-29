import os
import pandas as pd
from sqlalchemy import create_engine, engine
import psycopg2



class Database():

    def __init__(self):
        pass


    def create_engine_obj(database_conn_str: str) -> engine:
        return create_engine(database_conn_str)


    def load_df_to_db(df: pd.DataFrame, table_name: str, engine_obj: engine, schema_name: str = 'public') -> None:
        df.to_sql(table_name, engine_obj, if_exists='replace', schema=schema_name, chunksize=50000, method=None)


class Files():

    def __init__(self):
        pass

    def save_df_to_folder_csv(df: pd.DataFrame, folder_path: str, file_name: str) -> None:

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        df.to_csv(folder_path+file_name)
