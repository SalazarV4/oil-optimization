"""Script for defining classes for data extraction from the FRED and EIA API"""

from abc import ABC, abstractmethod
from typing import Any
import time
import pandas as pd
import requests
from dotenv import dotenv_values
from oil_optimization.utils.io_helpers import read_yaml, save_csv

SECRETS = dotenv_values('.env')
EIA_API_KEY = SECRETS['EIA_API_KEY']
FRED_API_KEY = SECRETS['FRED_API_KEY']

class Extractor(ABC):
    """Extractor base class, it's gonna contain 
    the base methods for api calls and saving files"""
    def __init__(self):
        self.config = read_yaml('config/config.yml')
        self.data_dir = self.config['data_ingestion']['root_dir']
        self.api_config = read_yaml('config/api_config.yml')

    def make_request(self, url:str, payload: dict[str,Any]):
        try:
            r = requests.get(url=url, params=payload, timeout=30)
            print(r.status_code)
            r.raise_for_status()
            time.sleep(3)
            return r.json()
        except requests.exceptions.HTTPError as e:
            print(e)

    @abstractmethod
    def extract_data(self, response_data):
        pass

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        path = f'{self.data_dir}/raw/{filename}.csv'
        save_csv(df=df, path=path)
        print(f'Datafile {filename} saved')


class EIAExtractor(Extractor):
    def __init__(self) -> None:
        super().__init__()
        self.eia_api_config = self.api_config['eia_api']

    def extract_data(self, response_data):
        return response_data['response']['data']

    def create_file(self, label:str, params:dict):
        data_list = []
        params['payload']['api_key'] = EIA_API_KEY
        if "date_intervals" in params.keys():
            for i, date in enumerate(params['date_intervals']):
                print(f'Label {i+1}: {label}')
                payload = params['payload'].copy()
                payload['start'] = date[0]
                if date[1]:
                    payload['end'] = date[1]
                data = self.make_request(params['url'], payload=payload)
                json_data = self.extract_data(data)
                df = pd.DataFrame(json_data)

                data_list.append(df)
            full_df = pd.concat(data_list)
            self.save_to_csv(full_df, label)
        else:
            data = self.make_request(params['url'], payload=params['payload'])
            json_data = self.extract_data(data)
            df = pd.DataFrame(json_data)
            self.save_to_csv(df, label)

class FREDExtractor(Extractor):
    def __init__(self):
        super().__init__()
        self.fred_api_config = self.api_config['fred_api']
        self.url = 'https://api.stlouisfed.org/fred/series/observations'

    def extract_data(self, response_data):
        return response_data['observations']

if __name__ == '__main__':
    eia_extractor = EIAExtractor()
    for key, params_dict in eia_extractor.eia_api_config.items():
        eia_extractor.create_file(label=key, params=params_dict)

    fred_extractor = FREDExtractor()
    for key, params_dict in fred_extractor.fred_api_config.items():
        params_dict['api_key'] = FRED_API_KEY
        fred_json = fred_extractor.make_request(url=fred_extractor.url, payload=params_dict)
        fred_data = fred_extractor.extract_data(fred_json)
        dataframe = pd.DataFrame(fred_data).rename({'date':'period'},axis=1) # Change of date name from 'date' to 'period'
        fred_extractor.save_to_csv(dataframe, key)
