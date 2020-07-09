import pandas as pd 
from neo4j import GraphDatabase 
from utils import df_tools 
from utils import cypher

class NodeFrame(pd.DataFrame):
    def __init__(self, data, id_col:str=None, lbl_col:str=None, labels=None):
        super().__init__(data)
        self.whatami = "I am a NeonPandas DataFrame"
        self.id_col = id_col
        self._set_node_labels(lbl_col=lbl_col, labels=labels)
        self._set_node_index()

    def show(self):
        return self.style.hide_index()
        
    def _set_node_labels(self, lbl_col:str=None, labels:set=None):
        """Part of NeonPandas DataFrame processing. Creates `labels` 
        column in DataFrame which is used in interaction with Neo4j."""
        _lbls = df_tools._merge_labels(self, column=lbl_col, labels=labels)
        # finish processing dataframe and labels column
        self.drop(columns=[lbl_col], inplace=True)
        # set labels as first column
        self.insert(0, 'labels', _lbls)
        return

    def _set_node_index(self):
        self['neo_idx'] = self.apply(
            lambda x: cypher.create_neo_match(x.labels, self.id_col, x[self.id_col]), axis=1)
        self.set_index('neo_idx', inplace=True)
        return

def read_csv(filepath:str, column:str=None, labels:set=None) -> NodeFrame:
    """Read neonpandas NodeFrame from csv file."""
    df = pd.read_csv(filepath)
    return NodeFrame(df, column=column, labels=labels)


class EdgeFrame(pd.DataFrame):
    def __init__(self, data, src_col:str='src', dest_col:str='dest', rel_col:str=None, rel_types:set=None):
        super().__init__(data)
        self.whatami = "NeonPandas EdgeFrame"
        self.src_col = src_col
        self.dest_col = dest_col
        self._set_relationship_types(rel_col=rel_col, rel_types=rel_types)
        
    def _set_relationship_types(self, rel_col:str=None, rel_types:set=None):
        rel_lbls = df_tools._merge_labels(self, column=rel_col, labels=rel_types)
        self.drop(columns=[rel_col], inplace=True)
        self.insert(0, 'rel_types', rel_lbls)
        return



class Graph:
    def __init__(self, uri:str, auth:tuple):
        self.uri = uri
        self.driver = GraphDatabase.driver(uri=self.uri, auth=auth)
        self.session = self.driver.session()
        
    def close(self):
        self.driver.close()

    def run(self, query:str, params:dict={}):
        result = self.session.run(query, params)
        return result

    def create_nodes(self, nodes:NodeFrame):
        """Create Nodes in Neo4j from input Pandas Dataframe."""
        if 'labels' not in nodes.columns:
            raise ValueError("Nodes DataFrame must contain 'labels' column. Use NeonPandas preprocessing first.")
        # prepare data for apoc
        apoc_nodes = df_tools.prepare_df_for_apoc(nodes)
        self.run(cypher.apoc_node_create(), {'nodes': apoc_nodes})

    def create_relationships(self, rels:pd.DataFrame, query:str, key:str='edges'):
        """Create Relationships in Neo4j from input Pandas DataFrame.
        Currently must provide a custom query to import dataframe
        into Neo4j as Relationship creation is more complicated than nodes."""
        apoc_rels = df_tools.convert_to_records(rels)
        self.run(query, {key: apoc_rels})


    def create_node_constraints(self, constrs:NodeFrame, labels:str='labels', prop_name:str='property'):
        for c in df_tools.convert_to_records(constrs):
            try:
                self.run(cypher.create_constraint_query(c.get(labels), c.get(prop_name)))
            except:
                raise RuntimeError("Error creating constraint on attr {lbls}.".format(c.get(labels)))
        return

    def semi_join(self, df:NodeFrame, on:str, labels={}) -> pd.DataFrame:
        try:
            result = self.run(cypher.bulk_node_exists_query(labels=labels, field=on), 
                                {'nodes': df_tools.convert_to_records(df)})
            return df_tools.neo_nodes_to_df(result)
        except:
            return pd.DataFrame()
        
    def anti_join(self, df:NodeFrame, on:str, labels={}) -> pd.DataFrame:
        y = self.semi_join(df, on, labels)
        return df_tools.anti_join(df, y[[on]], on=on)

    def match_nodes(self, labels:set={}, properties:dict={}, limit:int=None) -> pd.DataFrame:
        """Analogous to cypher match query; currently only queries
        for nodes with matching labels and properties, with option 
        to limit number of results. Ability to match relationships
        needs to be added later."""
        result = self.run(cypher.node_match_query(labels, properties, limit=limit))
        return df_tools.neo_nodes_to_df(result)