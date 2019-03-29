class DiscreteFilter:
    # Input the column name as an non-encoded string.
    # Input the values as a list, will be converted to an encoded set.
    def __init__(self, column_name, values_list):
        self.column_name = column_name
        self.values_set = set([x.encode() for x in values_list])
        self.column_index = None # This will be specified later.
