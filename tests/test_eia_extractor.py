import os
from tempfile import TemporaryDirectory
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from oil_optimization.utils.io_helpers import save_csv
from oil_optimization.data_pipeline.extractor import EIAExtractor

def test_save_csv():
    with TemporaryDirectory() as tmp_dir:
        tmp_file_path = os.path.join(tmp_dir,'data.csv')
        test_data = pd.DataFrame({'Name':['John','Alice','Rudy'],
                                  'Age':[15,20,35],
                                  'is_active':[True,False,True]})

        save_csv(test_data, tmp_file_path)

        assert os.path.exists(tmp_file_path)

@patch("requests.get")
def test_make_request(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'response':{'data':[{'date':'2015-01-01',
                             'value':100,
                             'units':'$/BBL'}]}
    }

    mock_get.return_value = mock_response

    extractor = EIAExtractor()
    df = extractor.make_request('fake_url',{'fake':'data'})

    assert isinstance(df, pd.DataFrame)
