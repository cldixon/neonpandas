import pandas as pd 
from pandas import DataFrame
from neonpandas.frames.nodeframe import NodeFrame
from neonpandas.utils import df_tools 
from neonpandas.utils.node_tools import Node

class EdgeFrame(DataFrame):
    def __init__(self, data, rel_col:str=None, 
                src_col:str='src', dest_col:str='dest', 
                src_lbl_col:str=None, dest_lbl_col:str=None,
                labels:set=None, src_lbls:set=None, dest_lbls:set=None):
        super(EdgeFrame, self).__init__(data)
        
        if rel_col:
            self.set_relationship(rel_col)
        
        if src_col or dest_col:
            self.src_col = self.set_src_column(src_col)
            self.dest_col = self.set_dest_column(dest_col)
        
        if any([labels, src_lbls, dest_lbls, src_lbl_col, dest_lbl_col]):
            self._construct_node_columns(labels, src_lbls, dest_lbls, src_lbl_col, dest_lbl_col)
    
    @property
    def _constructor(self):
        return EdgeFrame
    '''
    # this breaks EdgeFrame; not sure why right now
    @property
    def _constructor_sliced(self):
        return EdgeFrame
    '''
    
    def set_relationship(self, rel_col):
        assert rel_col in self
        self.rel_col = rel_col
        _rels = self[rel_col]
        self.drop(columns=[rel_col], inplace=True)
        self.insert(0, 'rel_type', _rels)
        return

    def _construct_node_columns(self, *args):
        return
    
    def _set_node_column(self, node_col:str):
        if isinstance(node_col, str):
            assert node_col in self
            return node_col 
        elif node_col is None:
            return node_col
        else:
            raise ValueError("'{}' column not found in EdgeFrame.".format(node_col))
            
    def set_src_column(self, src_col:str):
        return self._set_node_column(src_col)
    
    def set_dest_column(self, dest_col:str):
        return self._set_node_column(dest_col)

    def _create_nodes(self, col:str, lbl_col:str=None, labels:set=None):
        # create array of sets for node labels
        # TODO: this will be of the NodeSeries type eventually
        _lbls = df_tools._merge_labels(self, column=lbl_col, labels=labels)
        self[col] = [Node(lbl, self.src_col, x) for lbl,x in zip(_lbls, self[col])]
        return

    def to_nodeframe(self, id_col:str='node', labels:set=None):
        edges_df = pd.DataFrame(self)
        melted_edges = pd.melt(edges_df[[self.src_col, self.dest_col]],
                    var_name='direction', value_name=id_col).drop(columns=['direction'])
        nodes = melted_edges.drop_duplicates(subset=[id_col]).reset_index(drop=True)
        return NodeFrame(nodes, id_col=id_col, labels=labels)

    