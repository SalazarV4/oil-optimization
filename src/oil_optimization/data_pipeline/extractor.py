import logging
import sys
from abc import ABC, abstractmethod
from typing import Any
import time
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from dotenv import dotenv_values
from oil_optimization.utils.io_helpers import read_yaml

SECRETS = dotenv_values('.env')
EIA_API_KEY = SECRETS['EIA_API_KEY']
FRED_API_KEY = SECRETS['FRED_API_KEY']

logging.basicConfig(
                    level=logging.INFO,
                    format="%(asctime)s | %(name)s | %(levelname)s | %(lineno)d | %(message)s",
                    handlers=[
                    logging.FileHandler("myapp.log"),
                    logging.StreamHandler(sys.stdout),
                    ],
                    force=True
                    )
logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    def __init__(self):
        super().__init__()
        self.config = read_yaml("config/config.yml")
        self.data_dir = self.config["data_ingestion"]["data_dir"]
        self.api_config = read_yaml("config/api_config.yml")
        self.session = requests.Session()

        adapter = HTTPAdapter(max_retries=3)
        self.session.mount("https://", adapter)

    def get(self, url: str, method: str = "GET", payload: dict[str,Any] | None = None):
        try:
            if not payload:
                logger.warning("No query parameters are being sent to %s due to empty payload", url)

            r = self.session.request(method=method,
                                    url=url,
                                    timeout=20,
                                    params=payload)
            logger.info("Status code: %s", r.status_code)
            r.raise_for_status()
            time.sleep(3)

            if r.status_code == 200:
                return r.json()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)

    @abstractmethod
    def extract_data(self, response_data):
        pass

    @abstractmethod
    def create_file(self, label, params):
        pass

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        path = f'{self.data_dir}/raw/{filename}.csv'
        df.to_csv(path, index=False)
        logger.info("%s.csv successfully created!", filename)

    def close(self):
        self.session.close()
        logger.info("Session Closed.")

class EIAExtractor(BaseExtractor):
    def __init__(self, api_key: str | None = None):
        super().__init__()
        self.eia_api_config = self.api_config['eia_api']
        self.api_key = api_key

        logger.info("============================ Starting HTTP Session \
 for EIA API ============================")

    def extract_data(self, response_data):
        return response_data['response']['data']

    def create_file(self, label:str, params:dict[str,Any]):
        data_list = []
        params['payload']['api_key'] = self.api_key
        if "date_intervals" in params.keys():
            for i, date in enumerate(params['date_intervals']):
                payload = params['payload'].copy()
                payload['start'] = date[0]

                if date[1]:
                    payload['end'] = date[1]
                if i == 0:
                    logger.info("Limited data retrieval of %s from %s due to \
API limits, starting pagination...",
                                label,
                                params["url"])
                logger.info("Sending HTTP request #%s", i+1)
                data = self.get(params["url"], payload=payload)
                json_data = self.extract_data(data)

                data_list += json_data

            df = pd.DataFrame(data_list)
            self.save_to_csv(df, label)

        else:
            data = self.get(params['url'], payload=params['payload'])
            json_data = self.extract_data(data)
            df = pd.DataFrame(json_data)
            self.save_to_csv(df, label)

class FREDExtractor(BaseExtractor):
    def __init__(self, api_key: str | None = None):
        super().__init__()
        self.fred_api_config = self.api_config['fred_api']
        self.api_key = api_key
        self.url = self.fred_api_config["url"]
        self.payload_dicts = self.fred_api_config["payload"]

        logger.info("============================ Starting HTTP Session\
for FRED API ============================")

    def extract_data(self, response_data):
        return response_data['observations']

    def create_file(self, label: str, params: dict[str, Any]):
        params["api_key"] = self.api_key

        logger.info("Sending HTTP request for %s", label)
        data = self.get(url=self.url, payload=params)
        json_data = self.extract_data(data)
        df = pd.DataFrame(json_data).rename({"date":"period"}, axis=1)
        self.save_to_csv(df, label)

if __name__ == "__main__":
    eia_extractor = EIAExtractor(api_key=EIA_API_KEY)
    for key, params_dict in eia_extractor.eia_api_config.items():
        eia_extractor.create_file(label=key, params=params_dict)
    eia_extractor.close()

    fred_extractor = FREDExtractor(api_key=FRED_API_KEY)
    for key, params_dict in fred_extractor.payload_dicts.items():
        fred_extractor.create_file(label=key, params=params_dict)
    fred_extractor.close()
