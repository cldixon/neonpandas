from utils import df_tools

def format_attributes(attr, sep:str=':') -> str:
    attr = df_tools.conform_to_list(attr)
    return sep + sep.join(attr)

def apoc_node_create(key:str='nodes', var:str='n') -> str:
    """Returns cypher query for creating bulk nodes via APOC"""
    return """UNWIND ${key} as {var}
    CALL apoc.create.node({var}.attr, {var}.properties) YIELD  node
    RETURN COUNT(node)""".format(key=key, var=var)

def create_constraint_query(attr, property_name:str) -> str:
    """Returns query for creating unique node constraint in db."""
    return """CREATE CONSTRAINT
    ON (n{attr})
    ASSERT n.{prop_name}
    IS UNIQUE""".format(attr=format_attributes(attr), prop_name=property_name)


def bulk_node_match_query(field:str, attr:str=None) -> str:
    return """UNWIND $nodes AS node
    MATCH (n{attr} {{ {field}: node.{field} }})
    RETURN n.{field} AS {field}""".format(attr=format_attributes(attr), field=field)