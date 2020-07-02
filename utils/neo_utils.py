from utils import cypher 

def _run(tx, query,):
    return tx.run(query)

def _create_nodes_from_records(tx, apoc_records):
    tx.run(cypher.apoc_node_create(), nodes=apoc_records)

def _create_relationship_via_apoc(tx, apoc_rels:list, query:str):
    tx.run(query, edges=apoc_rels)

def _create_constraint(tx, attr, property_name):
    tx.run(cypher.create_constraint_query(), attr, property_name)

def _bulk_node_match(tx, nodes:list, query:str):
    result = tx.run(query, nodes=nodes)
    return result