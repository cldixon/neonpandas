from utils import df_tools

def format_labels(labels, sep:str=':') -> str:
    labels = df_tools.conform_to_set(labels)
    return sep + sep.join(labels)

def format_properties(properties:dict, sep:str=', ') -> str:
    props = {}
    for k,v in properties.items():
        if isinstance(v, str):
            props[k] = '"{}"'.format(v)
        else:
            props[k] = v
    return sep.join(['{}: {}'.format(k, v) for k,v in props.items()])

def apoc_node_create(key:str='nodes', var:str='n') -> str:
    """Returns cypher query for creating bulk nodes via APOC"""
    return """UNWIND ${key} as {var}
    CALL apoc.create.node({var}.labels, {var}.properties) YIELD  node
    RETURN COUNT(node)""".format(key=key, var=var)

def create_constraint_query(labels, property_name:str) -> str:
    """Returns query for creating unique node constraint in db."""
    return """CREATE CONSTRAINT
    ON (n{lbls})
    ASSERT n.{prop_name}
    IS UNIQUE""".format(lbls=format_labels(labels), prop_name=property_name)


def bulk_node_exists_query(field:str, labels:str=None, only_field:bool=False) -> str:
    query =  """UNWIND $nodes AS node
    MATCH (n{lbls} {{ {field}: node.{field} }})
    RETURN n""".format(lbls=format_labels(labels), field=field)
    if only_field is True:
        return query + '.{field} AS {field}'.format(field=field)
    else:
        return query

def node_match_query(labels:set=None, properties:dict={}, limit:int=None) -> str:
    labels, properties = format_labels(labels), format_properties(properties)
    q = """MATCH (n{lbls} {{ {props} }}) RETURN n""".format(lbls=labels, props=properties)
    if limit:
        q += ' LIMIT {}'.format(limit)
    return q