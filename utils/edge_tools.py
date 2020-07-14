import pandas as pd 
import neonpandas as npd 
from utils import node_tools

def _already_contains_nodes(col:pd.Series, num:int=3):
    for x in col[:num]:
        if not isinstance(x, node_tools.Node):
            return False
    return True
    
'''
def join_to_nodeframe(edges, nodes:npd.NodeFrame=None, src_nodes:npd.NodeFrame=None, dest_nodes:npd.NodeFrame=None):
    for _col,_nf in [(edges.src_col, src_nodes), (edges.dest_col, dest_nodes)]:
        nf = (_nf if nodes is None and _nf is not None else nodes)
        # convert (src|dest) node column to Node type
        if not _already_contains_nodes(edges[_col]):
            edges[_col] = edges[_col].apply(lambda x: node_tools.Node(nf.default_lbls, nf.id_col, x))
        # match to respective NodeFrame
        edges[_col] = edges[_col].apply(lambda x: node_tools.find_match(x, nf.index.to_series()))
    return edges
'''