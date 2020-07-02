import pandas as pd 

def conform_to_list(x) -> list:
    if isinstance(x, list):
        return x
    elif isinstance(x, str):
        return [x]
    elif x is None:
        return []
    else:
        raise ValueError("Input must be either str or list type.")

def convert_to_records(df:pd.DataFrame) -> list:
    """Convert a Pandas DataFrame to array of dictionaries
    (using Pandas 'to_dicv(orient='records')` method). This
    function also removes null/nan values from each 
    dictionary upon conversion."""
    return [{k:v for k,v in r.items() if pd.notnull(v)} for r in df.to_dict('records')]


def prepare_df_for_apoc(df:pd.DataFrame, attr:str=None, use_column:str=None) -> list:
    """Converts Pandas DataFrame to records for proper format for apoc command."""
    # parse attribute and column name inputs
    if attr and use_column:
        attr_func = lambda r: conform_to_list(attr) + [r.get(use_column)]
    elif attr and use_column is None:
        attr_func = lambda r: attr 
    elif use_column and attr is None:
        attr_func = lambda r: r.get(use_column)
    else:
        raise ValueError("Must provide either 'attr' or 'use_column' as input for attribute type.")
    # prepare input data
    return [{
        'attr': conform_to_list(attr_func(r)),
        'properties': {k:v for k,v in r.items() if k != use_column}}
    for r in convert_to_records(df)]