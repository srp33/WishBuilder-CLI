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

        self.__id = None
        self.__timestamp = None
        self.__description = None
        self.__title = None
        self.__num_samples = None
        self.__num_features = None
        self.__total_datapoints = None

    @property
    def id(self) -> str:
        if self.__id == None:
            self.__id = readStringFromFile(self.data_file_path, ".id").decode()
        return self.__id

    @property
    def timestamp(self) -> float:
        if self.__timestamp == None:
            self.__timestamp = readStringFromFile(self.data_file_path, ".timestamp").decode()
        return self.__timestamp

    @property
    def title(self) -> str:
        if self.__title == None:
            self.__title = readStringFromFile(self.data_file_path, ".title").decode()
        return self.__title

    @property
    def description(self) -> str:
        if self.__description == None:
            self.__description = readStringFromFile(self.data_file_path, ".desc").decode()
        return self.__description

    @property
    def num_samples(self) -> int:
        if self.__num_samples == None:
            self.__num_samples = readIntFromFile(self.data_file_path, ".nrow")
        return self.__num_samples

    @property
    def num_features(self) -> int:
        if self.__num_features == None:
            self.__num_features = readIntFromFile(self.data_file_path, ".ncol")
        return self.__num_features

    @property
    def total_datapoints(self):
        if self.__total_datapoints == None:
            self.__total_datapoints = self.num_samples * self.num_features
        return self.__total_datapoints

    # This function accepts filtering criteria, saves the matching row indices to a file,
    #   and returns the number of matching samples as well as the path to that file.
    # The input arguments must be of type DiscreteFilter or NumericFilter, respectively.
    # Make sure to delete the temp file after you are done with it!
    def save_sample_indices_matching_filters(self, discrete_filters, numeric_filters):
        # Prepare to parse data
        data_handle = openReadFile(self.data_file_path)
        ll = readIntFromFile(self.data_file_path, ".ll")
        cc_handle = openReadFile(self.data_file_path, ".cc")
        mccl = readIntFromFile(self.data_file_path, ".mccl")
        num_rows = self.num_samples
        cn_handle = openReadFile(self.data_file_path, ".cn")

        # Read the column names
        all_column_names = readStringsFromFile(self.data_file_path, ".cn")

        # Find the column names that will be queried across all filters
        filter_column_names = []
        for df in discrete_filters:
            filter_column_names.append(df.column_name.encode())
        for nf in numeric_filters:
            filter_column_names.append(nf.column_name.encode())

        # Find the indices associated with these columns
        filter_column_name_indices = get_indices_of_strings(all_column_names, filter_column_names)

        # Find rows that match discrete filtering criteria
        keep_row_indices = range(num_rows)
        for df in discrete_filters:
            keep_row_indices = self.filter_rows_discrete(keep_row_indices, df, filter_column_name_indices.pop(0), data_handle, cc_handle, mccl, ll)

        # Find rows that match numeric filtering criteria
        num_operator_dict = {">": operator.gt, "<": operator.lt, ">=": operator.ge, "<=": operator.le, "==": operator.eq, "!=": operator.ne}
        for nf in numeric_filters:
            keep_row_indices = self.filter_rows_numeric(keep_row_indices, nf, filter_column_name_indices.pop(0), num_operator_dict, data_handle, cc_handle, mccl, ll)

        # Save the row indices to a file
        keep_row_indices = [str(x).encode() for x in keep_row_indices]
        temp_file_path = generate_temp_file_path()
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(b"\n".join(keep_row_indices))

        data_handle.close()
        cc_handle.close()
        cn_handle.close()

        return len(keep_row_indices), temp_file_path

    # This function identifies which columns should be selected based on the specified
    #   columns, groups, and pathways. It returns the number of columns to be selected,
    #   a file path that contains the indices of the selected columns, and a file path
    #   that contains the names of the selected columns (in that order).
    # The input arguments should be lists of strings. If all the lists are empty, then
    #   all columns will be selected.
    # Make sure to delete the temp file after you are done with it!
    def save_column_indices_to_select(self, select_columns, select_groups, select_pathways):
        # Read the column names
        column_names = list(readStringsFromFile(self.data_file_path, ".cn"))

        # By default, select all columns
        if len(select_columns) == 0 and len(select_groups) == 0 and len(select_pathways) == 0:
            select_column_indices = range(len(column_names))
        else:
            # Find index of each individual column to be selected
            select_columns = [x.encode() for x in select_columns]
            select_column_indices = set([0] + get_indices_of_strings(column_names, select_columns))

            # Parse pathways and add corresponding genes to the list of columns that will be selected
            select_column_indices = select_column_indices | self.parse_indices_for_groups(self.data_file_path, ".pathways", select_pathways)

            # Find which columns to select based on groups
            select_column_indices = select_column_indices | self.parse_indices_for_groups(self.data_file_path, ".groups", select_groups)

        select_column_indices = sorted(list(select_column_indices))

        # Save the column indices to a file
        temp_file_path_indices = generate_temp_file_path()
        with open(temp_file_path_indices, "wb") as temp_file:
            temp_file.write(b"\n".join([str(i).encode() for i in select_column_indices]))

        # Save the column names to a file
        temp_file_path_names = generate_temp_file_path()
        with open(temp_file_path_names, "wb") as temp_file:
            temp_file.write(b"\t".join([column_names[i] for i in select_column_indices]))

        return len(select_column_indices), temp_file_path_indices, temp_file_path_names

    # This function retrieves data for the specified rows and columns and builds
    #   a file with the data. The first two arguments should be paths to files created
    #   using the above functions. The third argument indicates the path where the
    #   output file will be saved. The fourth argument is the type/format of the output file.
    # This function does not return anything.
    #### NOTE: Temporarily, tsv is the only supported option for the output file type.
    def build_output_file(self, row_indices_file_path, col_indices_file_path, col_names_file_path, out_file_path, out_file_type):
        row_indices = []
        if os.path.getsize(row_indices_file_path) > 0:
            row_indices = readIntsFromFile(row_indices_file_path)

        # This is a generator
        col_indices = readIntsFromFile(col_indices_file_path)

        # Prepare to parse data
        data_handle = openReadFile(self.data_file_path)
        ll = readIntFromFile(self.data_file_path, ".ll")
        cc_handle = openReadFile(self.data_file_path, ".cc")
        mccl = readIntFromFile(self.data_file_path, ".mccl")

        # Get the coords for each column to select
        select_column_coords = list(parse_data_coords(col_indices, cc_handle, mccl))

        # Write output file (in chunks)
        with open(out_file_path, 'wb') as out_file:
            # Header line
            out_file.write(readStringFromFile(col_names_file_path) + b"\n")

            out_lines = []
            chunk_size = 1000

            for row_index in row_indices:
                out_lines.append(b"\t".join([x.rstrip() for x in parse_data_values(row_index, ll, select_column_coords, data_handle)]))

                if len(out_lines) % chunk_size == 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
                    out_lines = []

            if len(out_lines) > 0:
                out_file.write(b"\n".join(out_lines) + b"\n")

        data_handle.close()
        cc_handle.close()

    # This is a convenience function, which acts as a wrapper around other functions.
    def query(self, discrete_filters, numeric_filters, select_columns, select_groups, select_pathways, out_file_path, out_file_type="tsv"):
        num_samples, row_indices_file_path = self.save_sample_indices_matching_filters(discrete_filters, numeric_filters)
        num_columns, col_indices_file_path, col_names_file_path = self.save_column_indices_to_select(select_columns, select_groups, select_pathways)

        self.build_output_file(row_indices_file_path, col_indices_file_path, col_names_file_path, out_file_path, out_file_type)

        if os.path.exists(row_indices_file_path):
            os.remove(row_indices_file_path)
        if os.path.exists(col_indices_file_path):
            os.remove(col_indices_file_path)

        return num_samples, num_columns

    ########################################################################
    # Treat these as private functions.
    ########################################################################

    def filter_rows_discrete(self, row_indices, the_filter, column_index, data_handle, cc_handle, mccl, ll):
        query_col_coords = list(parse_data_coords([column_index], cc_handle, mccl))

        for row_index in row_indices:
            if next(parse_data_values(row_index, ll, query_col_coords, data_handle)).rstrip() in the_filter.values_set:
                yield row_index

    def filter_rows_numeric(self, row_indices, the_filter, column_index, operator_dict, data_handle, cc_handle, mccl, ll):
        if the_filter.operator not in operator_dict:
            raise Exception("Invalid operator: " + oper)

        query_col_coords = list(parse_data_coords([column_index], cc_handle, mccl))

        for row_index in row_indices:
            value = next(parse_data_values(row_index, ll, query_col_coords, data_handle)).rstrip()
            if value == b"": # Is missing
                continue

            # See https://stackoverflow.com/questions/18591778/how-to-pass-an-operator-to-a-python-function
            if operator_dict[the_filter.operator](fastnumbers.float(value), the_filter.query_value):
                yield row_index

    def parse_indices_for_groups(self, fwf_file_path, file_extension, group_names):
        indices = set()

        if os.path.exists(fwf_file_path + file_extension):
            my_file = openReadFile(fwf_file_path, file_extension)

            for group_name in group_names:
                pattern = b"^" + re.escape(group_name).encode() + re.escape(b"\t")
                for line in iter(my_file.readline, b""):
                    if re.search(pattern, line):
                        indices = indices | set([int(x) for x in line.rstrip(b"\n").split(b"\t")[1].split(b",")])

            my_file.close()

        return indices
