from pathlib import Path
import yaml
import pandas as pd

def read_yaml(path_to_yaml: str):
    """Read yaml file"""
    with open(path_to_yaml, encoding='utf-8') as yaml_file:
        content = yaml.safe_load(yaml_file)
        return content

def save_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)
