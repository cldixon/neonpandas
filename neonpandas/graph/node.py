from pandas import Series
from neonpandas.graph import cypher 
from neonpandas.utils import df_tools

class Node:
    def __init__(self, labels:set, key:str, value, var:str='n'):
        self.labels = df_tools.conform_to_set(labels)
        self.key = key
        self.value = value
        self.var = (var if var is not None else 'n')
        
    def __repr__(self):
        return self.match()
    
    def __str__(self):
        return self.match()

    def __eq__(self, node) -> bool:
        if isinstance(node, self.__class__):
            return self.get_id() == node.get_id() and self.shares_labels(node)
        else:
            return False

    def shares_labels(self, node) -> bool:
        if isinstance(node.labels, set):
            return (True if self.labels.intersection(node.labels) else False)
        else:
            raise ValueError("Input node's labels must be of type set.")

    def num_labels(self) -> int:
        """Returns the number of labels for Node."""
        return len(self.labels)

    def get_id(self, key:str=None) -> dict:
        """Returns key:value pair of identifying property for Cypher query.
        Can provide a 'key' parameter to override node object's stored key."""
        key = (self.key if key is None else key)
        return {key: self.value}

    def match(self, n_lbls:int=None, var:str=None) -> str:
        print_val = ('"{}"'.format(self.value) if isinstance(self.value, str) else self.value)
        _lbls = cypher.format_labels(list(self.labels), n=n_lbls)
        var = (self.var if var is None else var)
        _template = '({var}{lbls} {{{k}: {v}}})'
        return _template.format(var=var, lbls=_lbls, k=self.key, v=print_val)


def find_match(x:Node, nodes) -> Node:
    for n in nodes:
        if x == n:
            return n
    return x

def contains_nodes(col:Series, num:int=3):
    for x in col[:num]:
        if not isinstance(x, Node):
            return False
    return True