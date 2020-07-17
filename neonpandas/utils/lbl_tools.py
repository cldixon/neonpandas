from pandas import DataFrame, Series
from neonpandas.utils.df_tools import conform_to_set


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