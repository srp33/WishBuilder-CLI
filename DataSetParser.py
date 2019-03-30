import fastnumbers
import mmap
import operator
import os
import sys
from DataSetHelper import *
from DiscreteFilter import *
from NumericFilter import *

class DataSetParser:
    def __init__(self, data_file_path):
        self.data_file_path = data_file_path

    def get_num_samples(self):
        return readIntFromFile(self.data_file_path, ".nrow")

    def get_num_variables(self):
        return readIntFromFile(self.data_file_path, ".ncol")

    def get_num_datapoints(self):
        return self.get_num_samples() * self.get_num_variables()

    # If all select_columns are the default, we will select all columns.
    def query(self, discrete_filters, numeric_filters, select_columns=[], select_groups=[], select_pathways=[]):
        # Read the column names and map them to indices
        cn_handle = openReadFile(self.data_file_path, ".cn")
        mcnl = readIntFromFile(self.data_file_path, ".mcnl")
        cn_handle = openReadFile(self.data_file_path, ".cn")

        column_name_index_dict = {}
        column_names = []
        for i, line in enumerate(iter(cn_handle.readline, b"")):
            line = line.rstrip()
            column_name_index_dict[line] = i
            column_names.append(line)

        # By default, select all columns
        if len(select_columns) == 0 and len(select_groups) == 0 and len(select_pathways) == 0:
            select_column_indices = column_name_index_dict.values()
        else:
            select_column_indices = set([0] + [column_name_index_dict[x.encode()] for x in select_columns])

            # Parse pathways and add them to the list of columns that will be selected
            if len(select_pathways) > 0:
                select_pathways = set(select_pathways)

                for pathway_name, gene_indices in parse_pathway_gene_indices(self.data_file_path).items():
                    if pathway_name in select_pathways:
                        for column_index in gene_indices:
                            select_column_indices.add(column_index)

            # Find which columns to select based on groups
            # This may be a bit slow if you have a lot of columns (see if you can optimize)
            if len(select_groups) > 0:
                select_groups = set([x.encode() for x in select_groups])
                for column_name in column_name_index_dict:
                    column_name_parts = column_name.split(b"__")
                    if column_name_parts[0] in select_groups:
                        select_column_indices.add(column_name_index_dict[column_name])

        select_column_indices = sorted(list(select_column_indices))

        # Prepare to parse data
        data_handle = openReadFile(self.data_file_path)
        ll = readIntFromFile(self.data_file_path, ".ll")
        cc_handle = openReadFile(self.data_file_path, ".cc")
        mccl = readIntFromFile(self.data_file_path, ".mccl")
        num_rows = self.get_num_samples()

        # Find rows that match discrete filtering criteria
        keep_row_indices = range(num_rows)
        for df in discrete_filters:
            df.column_index = column_name_index_dict[df.column_name.encode()]
            keep_row_indices = self.filter_rows_discrete(keep_row_indices, df, data_handle, cc_handle, mccl, ll)

        # Find rows that match numeric filtering criteria
        num_operator_dict = {">": operator.gt, "<": operator.lt, ">=": operator.ge, "<=": operator.le, "==": operator.eq, "!=": operator.ne}
        for nf in numeric_filters:
            nf.column_index = column_name_index_dict[nf.column_name.encode()]
            keep_row_indices = self.filter_rows_numeric(keep_row_indices, nf, num_operator_dict, data_handle, cc_handle, mccl, ll)

        # Get the coords for each column to select
        select_column_coords = list(parse_data_coords(select_column_indices, cc_handle, mccl))

        # Build output (list of lists for now)
        output = [[column_names[i] for i in select_column_indices]]
        for row_index in keep_row_indices:
            output.append([x.rstrip() for x in parse_data_values(row_index, ll, select_column_coords, data_handle)])

#        # Write TSV output (in chunks)
#        with open(out_file_path, 'wb') as out_file:
#            chunk_size = 1000
#            out_lines = [b"\t".join([x.encode() for x in select_columns])]
#
#            for row_index in keep_row_indices:
#                out_lines.append(b"\t".join(parse_data_values(row_index, ll, select_column_coords, data_handle)).rstrip())
#
#                if len(out_lines) % chunk_size == 0:
#                    out_file.write(b"\n".join(out_lines) + b"\n")
#                    out_lines = []
#
#            if len(out_lines) > 0:
#                out_file.write(b"\n".join(out_lines) + b"\n")

        data_handle.close()
        cc_handle.close()
        cn_handle.close()

        return output

    def filter_rows_discrete(self, row_indices, the_filter, data_handle, cc_handle, mccl, ll):
        query_col_coords = list(parse_data_coords([the_filter.column_index], cc_handle, mccl))

        for row_index in row_indices:
            if next(parse_data_values(row_index, ll, query_col_coords, data_handle)).rstrip() in the_filter.values_set:
                yield row_index

    def filter_rows_numeric(self, row_indices, the_filter, operator_dict, data_handle, cc_handle, mccl, ll):
        if the_filter.operator not in operator_dict:
            raise Exception("Invalid operator: " + oper)

        query_col_coords = list(parse_data_coords([the_filter.column_index], cc_handle, mccl))

        for row_index in row_indices:
            value = next(parse_data_values(row_index, ll, query_col_coords, data_handle)).rstrip()
            if value == b"": # Is missing
                continue

            # See https://stackoverflow.com/questions/18591778/how-to-pass-an-operator-to-a-python-function
            if operator_dict[the_filter.operator](fastnumbers.float(value), the_filter.query_value):
                yield row_index

    def get_num_samples_matching_filters(self):
        return 0

    def get_num_variables_matching_filters(self):
        return 0

    def get_num_datapoints_matching_filters(self):
        return 0
