from neonpandas.utils import df_tools
from neonpandas.frames.nodeframe import NodeFrame
from neonpandas.frames.edgeframe import EdgeFrame

#### Functions for transforming DataFrames into cypher-apoc format ####

## APOC Conversion for NodeFrames -----
def convert_nodes_to_apoc(nf:NodeFrame, lbls_col:str='labels') -> list:
    """
    Converts NodeFrame to array of dicts formatted for APOC commands.

    Cypher queries using the UNWIND expression require an array of data
    and the pre-written queries for neonpandas requires the dictionaries
    of data within that array to have a specific formatting. This function
    accomplished that transformation for a NodeFrame on its way to being
    added as nodes in Neo4j.

    Parameters
    ----------
    nf : NodeFrame
        A properly prepared NodeFrame object
    lbls_col : str
        Key name for node labels. This should be left alone.

    Returns
    -------
    list[dict]
        List of dictionaries, each properly formatted for bulk upload
        via pre-constructed query to Neo4j.
        
    """
    return [
        {'labels': df_tools.conform_to_list(r.get(lbls_col)),
         'properties': {k:v for k,v in r.items() if k != lbls_col}}
        for r in df_tools.convert_to_records(nf)]

## APOC Conversion for EdgeFrames ----
def convert_edges_to_apoc(ef:EdgeFrame):
    """
    Converts NeonPandas EdgeFrame to formatted array for APOC commands..

    Cypher queries using the UNWIND expression require an array of data
    and the pre-written queries for neonpandas requires the dictionaries
    of data within that array to have a specific formatting. This function
    accomplished that transformation for an EdgeFRame on its way to being
    added as edges in Neo4j.

    Parameters
    ----------
    ef : EdgeFrame
        A properly prepared EdgeFrame

    Returns
    -------
    list[dict]
        list of dictionaries, with each dictionary formated for cypher query.

    """
    non_property_columns = [ef.start_col, ef.end_col, ef.rel_col]
    edge_records = df_tools.convert_to_records(ef)
    apoc_edges = []
    for edge_rec in edge_records:
        start_node = edge_rec.get(ef.start_col)
        end_node = edge_rec.get(ef.end_col)
        apoc_data = {
            'rel_type': edge_rec.get(ef.rel_col),
            'start_id': start_node.get_id(),
            'end_id': end_node.get_id(),
            'start_lbls': list(start_node.labels),
            'end_lbls': list(end_node.labels),
            'properties' : {k:v for k,v in edge_rec.items() if k not in non_property_columns}
        }
        apoc_edges.append(apoc_data)
    return apoc_edges
