import pandas as pd 
from neo4j import GraphDatabase
from neonpandas.utils import df_tools
from neonpandas.utils import apoc_tools
from neonpandas.graph import queries
from neonpandas.frames.nodeframe import NodeFrame
from neonpandas.frames.edgeframe import EdgeFrame

class Graph:
    def __init__(self, uri:str, auth:tuple, encrypted: bool=False):
        self.uri = uri
        self.driver = GraphDatabase.driver(uri=self.uri, auth=auth, encrypted=encryptd)
        self.session = self.driver.session()
        
    def close(self):
        self.driver.close()

    def run(self, query:str, params:dict={}):
        result = self.session.run(query, params)
        return result

    def create_nodes(self, nf:NodeFrame):
        """Create Nodes in Neo4j from input Pandas Dataframe."""
        if nf.ready_for_upload():
            # prepare data for apoc
            apoc_nodes = apoc_tools.convert_nodes_to_apoc(nf)
            self.run(queries.apoc_node_create(), {'nodes': apoc_nodes})
        else:
            # TODO: This check will be more robust with introduction of LabelSeries
            raise ValueError("Nodes DataFrame must contain 'labels' column. Use NeonPandas preprocessing first.")
        return

    def create_edges(self, ef:EdgeFrame):
        if ef.ready_for_upload():
            # prepare data for apoc
            apoc_edges = apoc_tools.convert_edges_to_apoc(ef)
            self.run(queries.apoc_edge_create(), {'edges': apoc_edges})
        else:
            raise ValueError("Edgeframe is not yet ready for upload to Neo4j Graph.")
        return

    def create_node_constraints(self, constrs, labels:str='labels', prop_name:str='property'):
        for c in df_tools.convert_to_records(constrs):
            try:
                self.run(queries.create_constraint_query(c.get(labels), c.get(prop_name)))
            except:
                raise RuntimeError("Error creating constraint on attr {lbls}.".format(c.get(labels)))
        return

    def semi_join(self, df, on:str, labels={}) -> NodeFrame:
        result = self.run(queries.bulk_node_exists_query(labels=labels, field=on), 
                            {'nodes': df_tools.convert_to_records(df)})
        df = df_tools.neo_nodes_to_df(result)
        return NodeFrame(df)
        
    def anti_join(self, df, on:str, labels={}) -> NodeFrame:
        y = self.semi_join(df, on, labels)
        df = df_tools.anti_join(df, y[[on]], on=on)
        return NodeFrame(df)

    def match_nodes(self, labels:set={}, properties:dict={}, limit:int=None, *args, **kwargs) -> NodeFrame:
        """Analogous to cypher match query; currently only queries
        for nodes with matching labels and properties, with option 
        to limit number of results. Ability to match relationships
        needs to be added later."""
        result = self.run(queries.node_match_query(labels, properties, limit=limit))
        df = df_tools.neo_nodes_to_df(result)
        return NodeFrame(df, *args, **kwargs)