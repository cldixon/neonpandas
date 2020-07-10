from indexed import IndexedOrderedDict
    
class OrderedSet:
    def __init__(self, data:tuple):
        self._data = IndexedOrderedDict(tuple((i, data[i]) for i in range(len(data))))
        
    def _get_values(self):
        return tuple(self._data.values())
        
    def __getitem__(self, val):
        return self._get_values()[val]
    
    def __repr__(self):
        data = ["'{}'".format(x) if isinstance(x, str) else x for x in self._get_values()]
        return '{n}{{{d}}}'.format(n=self.__class__.__name__, d=', '.join(data))
    
    def __len__(self):
        return len(self._get_values())

    def add(self, val):
        self._data.update({len(self._data): val})
        return