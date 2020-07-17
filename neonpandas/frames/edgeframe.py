import pandas as pd 
from pandas import DataFrame
from neonpandas.graph import node
from neonpandas.utils import df_tools 
from neonpandas.utils import lbl_tools
from neonpandas.frames import nodeframe

class EdgeFrame(DataFrame):
    def __init__(self, data, rel_col:str=None, 
                src_col:str='src', dest_col:str='dest', 
                src_id:str=None, dest_id:str=None,
                src_lbl_col:str=None, dest_lbl_col:str=None,
                labels:set=None, src_lbls:set=None, dest_lbls:set=None):
        super(EdgeFrame, self).__init__(data)
        
        if rel_col:
            # set relationship type column
            self.set_relationship(rel_col)
        
        if src_col or dest_col:
            # define which columns contain src & dest nodes
            self.src_col = self.set_src_column(src_col)
            self.dest_col = self.set_dest_column(dest_col)

        if src_id or dest_id:
            # define 'key' name for each node (e.g. name, email_address, etc.)
            self.src_id = (src_id if src_id is not None else self.src_col)
            self.dest_id = (dest_id if dest_id is not None else self.dest_col)
        
        if any([labels, src_lbls, dest_lbls, src_lbl_col, dest_lbl_col]):
            # transform src & dest columns (and any input label information)
            # into Node columns. 
            # TODO: These colums will be a defined Series type.
            self.set_node_columns(labels, src_lbls, dest_lbls, src_lbl_col, dest_lbl_col)
    
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

    def set_node_columns(self, labels:set=None, 
                                src_lbls:set=None, dest_lbls:set=None, 
                                src_lbl_col:str=None, dest_lbl_col:str=None):
        """Convert src and dest columns into Node types. This will contain the identifying key-value
        properties, label set, and variable (e.g. n,b,etc.) for cypher query."""
        edge_inputs = [
            (self.src_col, self.src_id, src_lbls, src_lbl_col), 
            (self.dest_col, self.dest_id, dest_lbls, dest_lbl_col)
        ]

        for _dir, _id, _dir_lbls, _dir_lbl_col  in edge_inputs:
            # check against labels inputs
            if _dir_lbls is not None and labels is None:
                _lbl_set = _dir_lbls 
            elif labels is not None and _dir_lbls is None:
                _lbl_set = labels 
            else:
                raise ValueError("Do not provide input for both 'labels' & '{}'.".format(_dir))

            # merge labels and set as column in edgeframe
            _lbls = lbl_tools.merge_labels(self, _dir_lbl_col, _lbl_set)
            if _dir_lbl_col is None:
                _dir_lbl_col = '{}_labels'.format(_dir)
                self.insert(df_tools.get_column_idx(self, _dir) + 1, _dir_lbl_col, _lbls)
            else:
                self[_dir_lbl_col] = _lbls

            # transform id and labels columns into single node column
            self[_dir] = self.apply(lambda x: node.Node(x[_dir_lbl_col], _id, x[_dir], var=_dir[0]), axis=1)
            self.drop(columns=[_dir_lbl_col], inplace=True)
        return
    
    def _set_dir_column(self, node_col:str):
        if isinstance(node_col, str):
            assert node_col in self
            return node_col 
        elif node_col is None:
            return node_col
        else:
            raise ValueError("'{}' column not found in EdgeFrame.".format(node_col))
            
    def set_src_column(self, src_col:str):
        return self._set_dir_column(src_col)
    
    def set_dest_column(self, dest_col:str):
        return self._set_dir_column(dest_col)

    def ready_for_upload(self):
        if self.rel_col is None:
            return False
        for _col in [self.src_col, self.dest_col]:
            if not node.contains_nodes(self[_col]):
                return False
        return True

    def to_nodeframe(self, id_col:str='node', labels:set=None):
        """Transforms pair-columned EdgeFrame into NodeFrame
        containing all unique nodes found in src & dest columns."""
        edges_df = pd.DataFrame(self)
        melted_edges = pd.melt(edges_df[[self.src_col, self.dest_col]],
                    var_name='direction', value_name=id_col).drop(columns=['direction'])
        nodes = melted_edges.drop_duplicates(subset=[id_col]).reset_index(drop=True)
        return NodeFrame(nodes, id_col=id_col, labels=labels)

def load_edgeframe(filepath:str, *args, **kwargs) -> EdgeFrame:
    data = pd.read_csv(filepath)
    return EdgeFrame(data, *args, **kwargs)