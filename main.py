import json
import yaml
import pandas as pd
from pandas.io.json import json_normalize #package for flattening json in pandas df
import requests

from src.utils.utilities import Database, Files


def main():
    
    # load vars from file
    with open('src/configs/general_configs.yaml') as info:
                vars = yaml.load(info, Loader=yaml.FullLoader)

    folder_path = vars['FOLDER_PATH']
    database_conn_str = vars['DATABASE_CONN_STR']

    engine = Database.create_engine_obj(database_conn_str)
    
    # israel_flight_information
    israel_flight_information_url = 'https://data.gov.il/api/3/action/datastore_search?resource_id=e83f763b-b7d7-479e-b172-ae981ddc6de5&limit=1000'

    response = requests.get(israel_flight_information_url)
    if response.status_code == 200:
        data_json = json.loads(response.text)
        df1 = json_normalize(data_json['result']['records'])
    else:
        print(f'Error:  {str(response.status_code)}')

    Files.save_df_to_folder_csv(df=df1, folder_path=folder_path, file_name='israel_flight_information.csv')
    Database.load_df_to_db(df=df1, table_name='israel_flight_information', engine_obj=engine)



    # country_Codes
    country_Codes_url = 'http://api.geonames.org/countryInfoJSON?username=tzah'

    response = requests.get(country_Codes_url)
    if response.status_code == 200:
        data_json = json.loads(response.text)
        df2 = json_normalize(data_json['geonames'])
    else:
        print(f'Error:  {str(response.status_code)}')
    
    Files.save_df_to_folder_csv(df=df1, folder_path=folder_path, file_name='country_info.csv')
    Database.load_df_to_db(df=df2, table_name='country_info', engine_obj=engine)



if __name__ == '__main__':
    main()