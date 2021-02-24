import neo4j
import pandas as pd 
from pandas import DataFrame, Series
from neonpandas.utils import datetimes 

def conform_to_list(x) -> list:
    if isinstance(x, list):
        return x
    elif isinstance(x, str):
        return [x]
    elif isinstance(x, set):
        return list(x)
    elif isinstance(x, tuple):
        return list(x)
    elif x is None:
        return []
    else:
        raise ValueError("Input must be either str or list type.")

def conform_to_set(x) -> set:
    if isinstance(x, set):
        return x 
    elif isinstance(x, str):
        return {x}
    elif isinstance(x, list):
        return set(x)
    elif x is None:
        return {}
    else:
        raise ValueError("Input must be either str or set type.")


def prepare_record(r:dict) -> dict:
    r = {k:v for k,v in r.items() if pd.notnull(v)}
    if 'labels' in r:
        r['labels'] = conform_to_list(r.get('labels'))
    return r

def convert_to_records(df:pd.DataFrame, convert_datetimes:bool=False) -> list:
    """Convert a Pandas DataFrame to array of dictionaries
    (using Pandas 'to_dicv(orient='records')` method). This
    function also removes null/nan values from each 
    dictionary upon conversion."""
    return [prepare_record(r) for r in df.to_dict(orient='records')]
    

def anti_join(x:pd.DataFrame, y:pd.DataFrame, on:str) -> pd.DataFrame:
    if len(y) > 0:
        xay = pd.merge(left=x, right=y, on=on, how='left', indicator=True)
        xay = xay.loc[xay._merge == 'left_only', :].drop(columns='_merge').reset_index(drop=True)
        return xay 
    else:
        return x

def prepare_node(node:neo4j.graph.Node) -> dict:
    attr = {'labels': set(node.labels)}
    properties = dict(node.items())
    return {**attr, **properties}


def neo_nodes_to_df(neo_nodes) -> pd.DataFrame:
    nodes = list(neo_nodes.graph().nodes)
    return pd.DataFrame(prepare_node(n) for n in nodes)


def get_column_idx(df:pd.DataFrame, col_name:str) -> int:
    return df.columns.get_loc(col_name)


def merge_labels(df:DataFrame, column:str=None, labels:set=None) -> Series:
    """This function properly creates the labels column when creating a
    NodeFrame object."""
    if column is not None and labels is None:
        assert column in df.columns
        _lbls = df[column].apply(lambda x: conform_to_set(x))
    elif column is not None and labels is not None:
        assert column in df.columns
        _lbls = df[column].apply(lambda x: conform_to_set(labels).union(conform_to_set(x)))
    elif column is None and labels is not None:
        labels = conform_to_set(labels)
        _lbls = Series([labels for i in range(len(df))])
    else:
        raise ValueError("Must provide either 'labels' or 'column' as input for attribute type.")
    return _lbls