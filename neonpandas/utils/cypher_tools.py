## Utility functions for preparing data for Cypher queries.

def format_labels(labels, sep:str=':', n:int=None) -> str:
    """Formats node labels for neo4j MATCH statement."""
    _lbls = sep.join((labels if not n else labels[:n]))
    if _lbls != '':
        return sep + _lbls 
    else:
        return _lbls

def format_properties(properties:dict, sep:str=', ') -> str:
    props = {}
    for k,v in properties.items():
        if isinstance(v, str):
            props[k] = '"{}"'.format(v)
        else:
            props[k] = v
    return sep.join(['{}: {}'.format(k, v) for k,v in props.items()])