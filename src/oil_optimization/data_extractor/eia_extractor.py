import pandas as pd
import requests
import time
from src.oil_optimization.utils.io_helpers import read_yaml, save_csv

class EIAExtractor:
    def __init__(self, config_path:str, api_config_path, api:str = 'eia_api') -> None:
        self.config = read_yaml(config_path)
        self.eia_api_config = read_yaml(api_config_path)[api]
        self.data_dir = self.config['data_ingestion']['root_dir']
        self.dataframes = {}

    def _make_request(self, url: str, payload: dict, retries: int = 5, sleep: int = 5):
        for attempt in range(retries):
            try:
                r = requests.get(url, payload, timeout=10)
                print(f'Status Code: {r.status_code}')
                r.raise_for_status()
                return pd.DataFrame(r.json()['response']['data'])

            except requests.exceptions.HTTPError as e:
                print(e)
                time.sleep(sleep)


    def extract_data(self):
        for key, params_dict in self.eia_api_config.items():
            if "date_intervals" in params_dict.keys():
                for i, date in enumerate(params_dict['date_intervals']):
                    payload = params_dict['payload'].copy()
                    payload['start'] = date[0]
                    if date[1]:
                        payload['end'] = date[1]
                    df = self._make_request(params_dict['url'], payload=payload)

                    self.dataframes[key] = df

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        path = f'data/{filename}.csv'
        save_csv(df=df, path=path)
        print(f'Datafile {filename} saved!')
