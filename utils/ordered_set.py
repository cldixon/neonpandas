from indexed import IndexedOrderedDict
    
class OrderedSet:
    def __init__(self, data:tuple):
        #self._data = IndexedOrderedDict(tuple((i, data[i]) for i in range(len(data))))
        self._data = self._create_ordered_set(data)

    def _create_ordered_set(self, data):
        if isinstance(data, tuple):
            data = data
        elif isinstance(data, str):
            data = (data,)
        elif isinstance(data, list):
            data = tuple(data)
        else:
            raise ValueError("Input to OrderedSet not recognized. Can be a tuple or a string.")
        return IndexedOrderedDict(tuple((i, data[i]) for i in range(len(data))))
        
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

    def intersection(self, b) -> set:
        """Replicates Standard Set method of 'Intersection'.
        Returns a Set of elements existing in both self and 
        input 'b', which must also be an OrderedSet."""
        if len(self) > len(b):
            self,b = b,self
        c = []
        for x in self:
            if x in b:
                c.append(x)
        return set(c)

    def has_intersection(self, b) -> bool:
        """Similar to above intersection method,
        except this only returns a boolean;
        True once any intersection has been
        found and False if none are found."""
        if len(self) > len(b):
            self, b = b, self
        for x in self:
            if x in b:
                return True
        return False

    def tolist(self):
        return list(self._get_values())