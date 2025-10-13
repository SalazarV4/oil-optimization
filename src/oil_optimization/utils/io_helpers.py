from pathlib import Path
import yaml

def read_yaml(path_to_yaml: Path):
    with open(path_to_yaml, encoding='utf-8') as yaml_file:
        content = yaml.safe_load(yaml_file)
        return content
