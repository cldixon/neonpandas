import pandas as pd 
from neo4j.time import DateTime
from neonpandas.frames.nodeframe import NodeFrame

def detect_datetimes(x) -> bool:
    """Detects if a pandas series (i.e. dataframe column)
    is of the Datetime dtype."""
    return isinstance(x, pd.Timestamp)

def get_datetime_cols(df:pd.DataFrame) -> list:
    """Returns list of columns in DataFrame of DataTime dtype."""
    return [col for col,val in df.dtypes.iteritems() if detect_datetimes(val)]

def convert_to_neo_datetime(dt:pd.Timestamp) -> DateTime:
    """Converts pandas TimeStamp object to Neo4j DateTime Object."""
    return DateTime.from_iso_format(dt.isoformat())


def to_datetime(s:pd.Series, format:str = None) -> pd.Series:
    """Wrapper for pandas to_datetime method for converting
    and formatting datetime series. But additionally
    converts series from pandas datetime format to 
    Neo4j datetime format for importing."""
    dt_series = pd.to_datetime(s, format)
    return dt_series.apply(convert_to_neo_datetime)