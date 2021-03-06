import pandas as pd 
from pandas import DataFrame
from neonpandas.utils import df_tools 
from neonpandas.frames import styling

class NodeFrame(DataFrame):
    def __init__(self, data, id_col:str=None, lbl_col:str=None, labels:set=None):
        super(NodeFrame, self).__init__(data)

        # set column to regard as node identifier
        if id_col:
            self.set_id_column(id_col)

        # optional to construct labels column
        if lbl_col or labels:
            self.set_labels(lbl_col, labels)

    def show(self, num_rows:int=10):
        """Stylized printout of NodeFrame. Includes coloring of
        node label column. TODO: Needs to format defined `_id` column."""
        return styling.style_nodeframe(self, num_rows)

    @property
    def _constructor(self):
        return NodeFrame
    
    def set_id_column(self, id_col:str):
        if id_col in self:
            self.id_col = id_col
        elif id_col is None:
            self.id_col = None
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

    def ready_for_upload(self) -> bool:
        """Check if NodeFrame is ready for upload to Neo4j Graph."""
        return (True if 'labels' in self else False)


def load_nodeframe(filepath:str, *args, **kwargs) -> NodeFrame:
    """Read neonpandas NodeFrame from csv file."""
    df = pd.read_csv(filepath)
    return NodeFrame(df, *args, **kwargs)
