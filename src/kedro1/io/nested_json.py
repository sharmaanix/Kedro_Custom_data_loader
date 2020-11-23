from os.path import isfile
from typing import Any, Dict, List

import json
import pandas as pd
# from pandas.io.json import json_normalize

from kedro.io import AbstractDataSet

class NestedJSONDataSet(AbstractDataSet):

    def __init__(
        self,
        filepath: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        nested_key: List[str] = None 
    ) -> None:
        
        self._filepath = filepath
        
        default_save_args = {}
        default_load_args = {}

        self._load_args = {**default_load_args, **load_args} \
            if load_args is not None else default_load_args
        self._save_args = {**default_save_args, **save_args} \
            if save_args is not None else default_save_args
        
        self._nested_key = nested_key

    def _describe(self) -> Dict[str, Any]:
        return dict(filepath=self._filepath,
                    load_args=self._load_args,
                    save_args=self._save_args,
                    nested_key= self._nested_key)
    
    def _exists(self) -> bool:
        return isfile(self._filepath)


    def _load(self) -> None:
        return self.normalize_json()


    def _save(self, data: pd.DataFrame) -> None:
        data.to_csv(**self._save_args)
    
    def normalize_json(self):
        with open(self._filepath) as json_file:
            data = json.load(json_file)
        temp_json = data

        for key in self._nested_key:
            if isinstance(temp_json, dict):
                temp_json = temp_json[key]
            elif isinstance(temp_json, list):
                temp_json = temp_json[0][key]
            else:
                assert "Type Error: Datatype not found"
        #JSON normalization
        df1 = pd.json_normalize(temp_json)
        del data[self._nested_key[0]]
        df2 = pd.DataFrame(data)
        return  pd.concat([df2, df1], axis=1)

    