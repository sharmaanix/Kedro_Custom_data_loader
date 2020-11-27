import json 
import pandas as pd
from typing import Any, Dict, List

def extract_json(filepath: str, nested_key: List) -> pd.DataFrame:
    with open(filepath) as json_file:
        data = json.load(json_file)
        temp_json = data

    for key in nested_key:
        if isinstance(temp_json, dict):
            temp_json = temp_json[key]
        elif isinstance(temp_json, list):
            temp_json = temp_json[0][key]
        else:
            assert "Type Error: Datatype not found"
    #JSON normalization
    df1 = pd.json_normalize(temp_json)
    del data[nested_key[0]]
    df2 = pd.DataFrame(data)
    return  pd.concat([df2, df1], axis=1)


# data = extract_json(filepath="data/01_raw/distance.json",nested_key= ['rows','elements'])
# print(data)