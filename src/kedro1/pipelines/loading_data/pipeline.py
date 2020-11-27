from kedro.pipeline import node, Pipeline

# from kedro1.pipelines.loading_data import extract_json
from .nodes import extract_json

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=extract_json,
                inputs=['params:filepath', 'params:nested_key' ],
                outputs="extracted_json",
            )
        ]
    )