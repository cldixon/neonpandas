from utils import cypher 
from utils import df_tools
from utils.ordered_set import OrderedSet

class Node:
    def __init__(self, labels:tuple, key, value, var:str='n'):
        self.labels = OrderedSet(df_tools.conform_to_tuple(labels))
        self.key = key
        self.value = value
        self.var = var
        
    def __repr__(self):
        return self.match()
    
    def __str__(self):
        return self.match()

    def num_labels(self) -> int:
        """Returns the number of labels for Node."""
        return len(self.labels)

    def match(self, n_lbls:int=None, var:str=None) -> str:
        print_val = ('"{}"'.format(self.value) if isinstance(self.value, str) else self.value)
        _lbls = cypher.format_labels(self.labels, n=n_lbls)
        var = (self.var if var is None else var)
        _template = '({var}{lbls} {{{k}: {v}}})'
        return _template.format(var=var, lbls=_lbls, k=self.key, v=print_val)