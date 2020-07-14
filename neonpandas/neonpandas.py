import pandas as pd 
from pandas import DataFrame, Series
from neo4j import GraphDatabase 
from utils import df_tools 
from utils import cypher
from utils import node_tools
from utils import edge_tools


class NodeFrame(DataFrame):
    def __init__(self, data, id_col:str=None, lbl_col:str=None, labels:set=None):
        super(NodeFrame, self).__init__(data)
        self.id_col = self.set_id_column(id_col)
        # optional to construct labels column
        if lbl_col or labels:
            self.set_labels(lbl_col, labels)
    
    @property
    def _constructor(self):
        return NodeFrame
    
    def set_id_column(self, id_col:str):
        if id_col in self:
            return id_col
        elif id_col is None:
            return None
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


def read_csv(filepath:str, id_col:str=None, lbl_col:str=None, labels:tuple=None) -> NodeFrame:
    """Read neonpandas NodeFrame from csv file."""
    df = pd.read_csv(filepath)
    return NodeFrame(df, id_col=id_col, lbl_col=lbl_col, labels=labels)


class EdgeFrame(DataFrame):
    def __init__(self, data, rel_col:str=None, src_col:str='src', dest_col:str='dest'):
        super(EdgeFrame, self).__init__(data)
        self.rel_col = rel_col
        if self.rel_col:
            self.set_relationship(self.rel_col)
        if src_col or dest_col:
            self.src_col = self.set_src_column(src_col)
            self.dest_col = self.set_dest_column(dest_col)
    
    @property
    def _constructor(self):
        return EdgeFrame
    
    def set_relationship(self, rel_col):
        assert rel_col in self
        _rels = self[rel_col]
        self.drop(columns=[rel_col], inplace=True)
        self.insert(0, 'rel_type', _rels)
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