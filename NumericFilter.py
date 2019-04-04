class NumericFilter:
    # Input the column name as an non-encoded string.
    # Operator must be <, <=, >, >=, ==, or !=
    # Query value must numeric (float or int)
    def __init__(self, column_name, operator, query_value):
        self.column_name = column_name
        self.operator = operator
        self.query_value = query_value
