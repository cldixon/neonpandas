from neonpandas.utils import df_tools
from neonpandas.frames.nodeframe import NodeFrame
from neonpandas.frames.edgeframe import EdgeFrame

#### Tools for transforming DataFrames into cypher-apoc format ####

## APOC Conversion for NodeFrames -----
def convert_nodes_to_apoc(nf:NodeFrame, lbls_col:str='labels') -> list:
    """Converts NeonPandas NodeFrame to formatted array for APOC commands."""
    return [
        {'labels': df_tools.conform_to_list(r.get(lbls_col)),
         'properties': {k:v for k,v in r.items() if k != lbls_col}}
        for r in df_tools.convert_to_records(nf)]

## APOC Conversion for EdgeFrames ----
def convert_edges_to_apoc(ef:EdgeFrame):
    """Converts NeonPandas EdgeFrame to formatted array for APOC commands."""
    non_property_columns = [ef.src_col, ef.dest_col, ef.rel_col]
    edge_records = df_tools.convert_to_records(ef)
    apoc_edges = []
    for edge_rec in edge_records:
        src_node = edge_rec.get(ef.src_col)
        dest_node = edge_rec.get(ef.dest_col)
        apoc_data = {
            'rel_type': edge_rec.get(ef.rel_col),
            'src_id': src_node.get_id(),
            'dest_id': dest_node.get_id(),
            'src_lbls': list(src_node.labels),
            'dest_lbls': list(dest_node.labels),
            'properties' : {k:v for k,v in edge_rec.items() if k not in non_property_columns}
        }
        apoc_edges.append(apoc_data)
    return apoc_edges
