import pandas as pd 
from pandas import DataFrame
from neonpandas.graph import node
from neonpandas.utils import df_tools 
from neonpandas.utils import lbl_tools
from neonpandas.frames import nodeframe
from neonpandas.frames import styling

class EdgeFrame(DataFrame):
    def __init__(self, data, rel_col:str=None, 
                start_col:str='start', end_col:str='end', 
                start_id:str=None, end_id:str=None,
                start_lbl_col:str=None, end_lbl_col:str=None,
                labels:set=None, start_lbls:set=None, end_lbls:set=None):
        super(EdgeFrame, self).__init__(data)
        
        if rel_col:
            # set relationship type column
            self.set_relationship(rel_col)
        
        if (start_col and end_col) and not self._has_formatted_nodes():
            # define which columns contain start & end nodes
            self.start_col = self.set_start_column(start_col)
            self.end_col = self.set_end_column(end_col)

        if start_id or end_id:
            # define 'key' name for each node (e.g. name, email_address, etc.)
            self.start_id = (start_id if start_id is not None else self.start_col)
            self.end_id = (end_id if end_id is not None else self.end_col)
        
        if any([labels, start_lbls, end_lbls, start_lbl_col, end_lbl_col]):
            # transform start & end columns (and any input label information)
            # into Node columns. 
            # TODO: These colums will be a defined Series type.
            self.set_node_columns(labels, start_lbls, end_lbls, start_lbl_col, end_lbl_col)

    def show(self, num_rows:int=10):
        """Stylized printout of EdgeFrame. 
        Formats Relationship-Type column."""
        return styling.style_edgeframe(self, num_rows)

    @property
    def _constructor(self):
        return EdgeFrame

    '''
    # this breaks EdgeFrame; not sure why right now
    @property
    def _constructor_sliced(self):
        return EdgeFrame
    '''
    
    def set_relationship(self, rel_col, col_idx:int=0):
        #assert rel_col in self
        self.rel_col = rel_col
        _rels = self[rel_col]
        self.drop(columns=[rel_col], inplace=True)
        self.insert(col_idx, 'rel_type', _rels)
        return

    def set_node_columns(self, labels:set=None, 
                                start_lbls:set=None, end_lbls:set=None, 
                                start_lbl_col:str=None, end_lbl_col:str=None,
                                start_col_idx:int=0, end_col_idx:int=2):
        """Convert start and end columns into Node types. This will contain the identifying key-value
        properties, label set, and variable (e.g. n,b,etc.) for cypher query."""
        edge_inputs = [
            (self.start_col, self.start_id, start_lbls, start_lbl_col, start_col_idx), 
            (self.end_col, self.end_id, end_lbls, end_lbl_col, end_col_idx)
        ]

        for _dir, _id, _dir_lbls, _dir_lbl_col, _idx  in edge_inputs:
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

            # transform id and labels columns into single node columns

            self[_dir] = self.apply(lambda x: node.Node(x[_dir_lbl_col], _id, x[_dir], var=_dir[0]), axis=1)
            self.drop(columns=[_dir_lbl_col], inplace=True)

            ## keeping this here!!!
            ## below re-orders columns to be like (start)-[rel]-(end)
            ## just comment out above two lines and replace with below
            ## depracated for now but may use later as it is aligned
            ## with cypher cyntax
            #_dir_nodes = self.apply(lambda x: node.Node(x[_dir_lbl_col], _id, x[_dir], var=_dir[0]), axis=1)
            #self.drop(columns=[_dir_lbl_col, _dir], inplace=True)
            #self.insert(_idx, _dir, _dir_nodes)
        return
    
    def _set_dir_column(self, node_col:str):
        if isinstance(node_col, str):
            #assert node_col in self
            return node_col 
        elif node_col is None:
            return node_col
        else:
            raise ValueError("'{}' column not found in EdgeFrame.".format(node_col))
            
    def set_start_column(self, start_col:str):
        return self._set_dir_column(start_col)
    
    def set_end_column(self, end_col:str):
        return self._set_dir_column(end_col)

    def _has_formatted_nodes(self) -> bool:
        try:
            if node.contains_nodes(self[self.start_col]) and node.contains_nodes(self[self.end_col]):
                return True
            else:
                return False
        except:
            return False

    def ready_for_upload(self):
        if self.rel_col is None:
            return False
        for _col in [self.start_col, self.end_col]:
            if not node.contains_nodes(self[_col]):
                return False
        return True

    def to_nodeframe(self, id_col:str='node', labels:set=None):
        """Transforms pair-columned EdgeFrame into NodeFrame
        containing all unique nodes found in start & end columns."""
        edges_df = pd.DataFrame(self)
        melted_edges = pd.melt(edges_df[[self.start_col, self.end_col]],
                    var_name='direction', value_name=id_col).drop(columns=['direction'])
        nodes = melted_edges.drop_duplicates(subset=[id_col]).reset_index(drop=True)
        return NodeFrame(nodes, id_col=id_col, labels=labels)

def load_edgeframe(filepath:str, *args, **kwargs) -> EdgeFrame:
    data = pd.read_csv(filepath)
    return EdgeFrame(data, *args, **kwargs)