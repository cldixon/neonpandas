import pandas as pd 
from pandas import DataFrame
from neonpandas.utils import df_tools 

class NodeFrame(DataFrame):
    def __init__(self, data, id_col:str=None, lbl_col:str=None, labels:set=None):
        super(NodeFrame, self).__init__(data)
        self.id_col = self.set_id_column(id_col)
        # optional to construct labels column
        if lbl_col or labels:
            self.set_labels(lbl_col, labels)
    
    @property
    def _constructor(self):
        return NodeFrame
    
    def set_id_column(self, id_col:str):
        if id_col in self:
            return id_col
        elif id_col is None:
            return None
        else:
            raise ValueError("Column '{}' not in NodeFrame.".format(id_col))
        return
    
    def set_labels(self, lbl_col:str=None, labels:set=None):
        if lbl_col is not None and labels is None:
            assert lbl_col in self.columns
            _lbls = self[lbl_col].apply(lambda x: df_tools.conform_to_set(x))
        elif lbl_col is not None and labels is not None:
            assert lbl_col in self.columns
            _lbls = self[lbl_col].apply(lambda x: df_tools.conform_to_set(labels).union(df_tools.conform_to_set(x)))
        elif lbl_col is None and labels is not None:
            labels = df_tools.conform_to_set(labels)
            _lbls = [labels for i in range(len(self))]
        else:
            raise ValueError("Must provide either 'labels' or 'column' as input for attribute type.")
        if lbl_col in self:
            self.drop(columns=[lbl_col], inplace=True)
        self.insert(0, 'labels', _lbls)
        return

def read_csv(filepath:str, id_col:str=None, lbl_col:str=None, labels:tuple=None) -> NodeFrame:
    """Read neonpandas NodeFrame from csv file."""
    df = pd.read_csv(filepath)
    return NodeFrame(df, id_col=id_col, lbl_col=lbl_col, labels=labels)
