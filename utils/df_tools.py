import neo4j
import pandas as pd 
from utils.node import Node

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

def conform_to_tuple(x) -> tuple:
    if isinstance(x, tuple):
        return x
    elif isinstance(x, str):
        return (x,)
    elif isinstance(x, list):
        return tuple(x)
    elif isinstance(x, set):
        return tuple(x)
    else:
        raise ValueErro("Invalid input for 'x'. Couldn't convert to tuple.")


def prepare_record(r:dict) -> dict:
    r = {k:v for k,v in r.items() if pd.notnull(v)}
    if 'labels' in r:
        r['labels'] = conform_to_list(r.get('labels'))
    return r

def convert_to_records(df:pd.DataFrame) -> list:
    """Convert a Pandas DataFrame to array of dictionaries
    (using Pandas 'to_dicv(orient='records')` method). This
    function also removes null/nan values from each 
    dictionary upon conversion."""
    return [prepare_record(r) for r in df.to_dict(orient='records')]

def prepare_df_for_apoc(npd_df:pd.DataFrame, lbls_col:str='labels') -> list:
    """Converts NeonPandas Dataframe to formatted array for APOC commands."""
    return [
        {'labels': conform_to_list(r.get(lbls_col)),
         'properties': {k:v for k,v in r.items() if k != lbls_col}}
        for r in convert_to_records(npd_df)]


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

def neo_nodes_to_df(neo_nodes:neo4j.work.result.Result) -> pd.DataFrame:
    nodes = list(neo_nodes.graph().nodes)
    return pd.DataFrame(prepare_node(n) for n in nodes)

def _merge_labels(df:pd.DataFrame, column:str=None, labels:tuple=None) -> pd.Series:
    """This function properly creates the labels column when creating a
    NodeFrame object or the relationship types when creating an EdgeFrame
    object."""
    if column is not None and labels is None:
        assert column in df.columns
        _lbls = df[column].apply(lambda x: conform_to_tuple(x))
    elif column is not None and labels is not None:
        assert column in df.columns
        _lbls = df[column].apply(lambda x: conform_to_tuple(labels) + conform_to_tuple(x))
    elif column is None and labels is not None:
        labels = conform_to_tuple(labels)
        _lbls = [labels for i in range(len(df))]
    else:
        raise ValueError("Must provide either 'labels' or 'column' as input for attribute type.")
    return _lbls

def _generate_node_idx(df:pd.DataFrame, key:str, value_col:str=None, lbls_col:str='labels', var:str='n') -> pd.Series:
    # if value is not provided, assume key and value are the same; the column name matches
    value_col = (value_col if value_col is not None else key)
    if value_col not in df:
        raise ValueError("Input '_id_col' must be a column in the input DataFrame.")
    return df.apply(lambda x: Node(x[lbls_col], key, x[value_col], var=var), axis=1)
