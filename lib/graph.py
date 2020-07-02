import pandas as pd 
from neo4j import GraphDatabase 
from utils import df_tools 
from utils import cypher 
from utils import neo_utils

class Graph:
    def __init__(self, uri:str, auth:tuple):
        self.uri = uri
        self.driver = GraphDatabase.driver(uri=self.uri, auth=auth)
        
    def close(self):
        self.driver.close()
        
    def _write(self, func, data):
        with self.driver.session() as session:
            session.write_transaction(func, data)

    def run(self, query):
        return self._write(neo_utils._run, query)


    def _read(self, func, data):
        with self.driver.session() as session:
            session.read_transaction(func, data)
    
    def create_nodes(self, nodes:pd.DataFrame, attr:str=None, use_column:str=None):
        """Create Nodes in Neo4j from input Pandas Dataframe."""
        # prepare data for apoc
        apoc_records = df_tools.prepare_df_for_apoc(nodes, attr=attr, use_column=use_column)
        self._write(neo_utils._create_nodes_from_records, apoc_records)

    def create_relationships(self, rels:pd.DataFrame, query:str):
        """Create Relationships in Neo4j from input Pandas DataFrame.
        Currently must provide a custom query to import dataframe
        into Neo4j as Relationship creation is more complicated than nodes."""
        apoc_rels = df_tools.convert_to_records(rels)
        with self.driver.session() as session:
            session.write_transaction(neo_utils._create_relationship_via_apoc, apoc_rels, query)


    def create_node_constraints(self, constraints:pd.DataFrame, attr:str='attr', property_name:str='property'):
        for c in df_tools.convert_to_records(constraints):
            self.run(cypher.create_constraint_query(c.get(attr), c.get(property_name)))
        return