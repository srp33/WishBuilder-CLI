from fastnumbers import *
import mmap
import os
import sys

def convert_tsv_to_fwf(file_path, out_file_path):
    column_size_dict = {}
    column_start_coords = []
    column_types_dict = {}

    # Initialize a dictionary with the column index as key and width of the column as value
    with open(file_path, 'rb') as my_file:
        col_names = my_file.readline().rstrip(b"\n").split(b"\t")

        for i in range(len(col_names)):
            column_size_dict[i] = 0

    # Iterate through the lines to find the max width and type of each column
    with open(file_path, 'rb') as my_file:
        # Ignore the header line because we saved column names elsewhere
        my_file.readline()

        for line in my_file:
            line_items = line.rstrip(b"\n").split(b"\t")

            for i in range(len(line_items)):
                column_size_dict[i] = max([column_size_dict[i], len(line_items[i])])

                if not i in column_types_dict:
                    # This is the header line, so we don't need to check its type
                    column_types_dict[i] = set()
                else:
                    column_types_dict[i].add(get_type(line_items[i]))

    # Calculate the length of the first line (and thus all the other lines)
    line_length = sum([column_size_dict[i] for i in range(len(col_names))])

    # Save value that indicates line length
    writeStringToFile(out_file_path, ".ll", str(line_length + 1).encode())

    # Calculate the positions where each column starts
    cumulative_position = 0
    for i in range(len(col_names)):
        column_size = column_size_dict[i]
        column_start_coords.append(str(cumulative_position))
        cumulative_position += column_size

    # Build a map of the header items and save this to a file
    col_names_string, max_col_name_length = buildStringMap([x.decode() for x in col_names])
    writeStringToFile(out_file_path, ".cn", col_names_string)
    writeStringToFile(out_file_path, ".mcnl", max_col_name_length)

    # Calculate the column coordinates and max length of these coordinates
    column_coords_string, max_column_coord_length = buildStringMap(column_start_coords)

    # Save column coordinates
    writeStringToFile(out_file_path, ".cc", column_coords_string)

    # Save value that indicates maximum length of column coords string
    writeStringToFile(out_file_path, ".mccl", max_column_coord_length)

    # Find most generic data type for each column
    column_types = []
    for i, types in sorted(column_types_dict.items()):
        column_types.append(get_most_generic_type(types))

    # Calculate the column types and max length of these types
    column_types_string, max_column_types_length = buildStringMap(column_types)

    # Save column types
    writeStringToFile(out_file_path, ".ct", column_types_string)

    # Save value that indicates maximum length of column types
    writeStringToFile(out_file_path, ".mctl", max_column_types_length)

    # Save the data to output file
    with open(file_path, 'rb') as my_file:
        # Ignore the header line because we saved column names elsewhere
        my_file.readline()

        with open(out_file_path, 'wb') as out_file:
            cumulative_position = 0

            for line in my_file:
                line_items = line.rstrip(b"\n").split(b"\t")

                line_out = b""
                for i in sorted(column_size_dict.keys()):
                    line_out += format_string(line_items[i], column_size_dict[i])
                cumulative_position += len(line_out)

                # This newline character is unnecessary, so it adds a bit of disk space.
                # However, it makes the files much more readable to humans.
                out_file.write(line_out + b"\n")

def get_type(a_string):
    return ("s", "n")[isfloat(a_string)] #string or number

def get_most_generic_type(types_set):
    if len(types_set) > 1:
        return "s" # string

    return list(types_set)[0]

def format_string(x, size):
    formatted = "{:<" + str(size) + "}"
    return formatted.format(x.decode()).encode()

def parse_data_coords(line_indices, coords_file, coords_file_max_length, full_str_length):
    coords_file_length = len(coords_file)
    out_dict = {}

    for index in line_indices:
        start_pos = index * (coords_file_max_length + 1)
        next_start_pos = start_pos + coords_file_max_length + 1
        further_next_start_pos = min(next_start_pos + coords_file_max_length, coords_file_length)

        if start_pos in out_dict:
            data_start_pos = out_dict[start_pos]
        else:
            data_start_pos = int(coords_file[start_pos:next_start_pos].rstrip())

        if next_start_pos == further_next_start_pos:
            data_end_pos = full_str_length
        else:
            if next_start_pos in out_dict:
                data_end_pos = out_dict[next_start_pos]
            else:
                data_end_pos = int(coords_file[next_start_pos:further_next_start_pos].rstrip())

        yield [index, data_start_pos, data_end_pos]

def parse_data_values(start_offset, segment_length, data_coords, str_like_object, end_offset=0):
    start_pos = start_offset * segment_length

    for coords in data_coords:
        yield str_like_object[(start_pos + coords[1]):(start_pos + coords[2] + end_offset)]

def getMaxStringLength(the_list):
    return max([len(str(x)) for x in set(the_list)])

def buildStringMap(the_list):
    # Find maximum length of value
    max_value_length = getMaxStringLength(the_list)

    # Build output string
    output = ""
    formatter = "{:<" + str(max_value_length) + "}\n"
    for value in the_list:
        output += formatter.format(str(value))

    return output.encode(), str(max_value_length).encode()

def readIntFromFile(file_path, file_extension=""):
    with open(file_path + file_extension, 'rb') as the_file:
        return int(the_file.read().rstrip())

def writeStringToFile(file_path, file_extension, the_string):
    with open(file_path + file_extension, 'wb') as the_file:
        the_file.write(the_string)

def openReadFile(file_path, file_extension=""):
    the_file = open(file_path + file_extension, 'rb')
    return mmap.mmap(the_file.fileno(), 0, prot=mmap.PROT_READ)

def countFileLines(file_path, file_extension=""):
    num_lines = 0

    with open(file_path + file_extension, 'rb') as the_file:
        for line in the_file:
            num_lines += 1

    return num_lines

def parse_column_values(meta, row_start_index, col_index):
    col_coords = [meta["col_coords"][col_index]]
    for row_index in range(row_start_index, meta["data_num_rows"]):
        yield next(parse_data_values(row_index, meta["ll"], col_coords, meta["data_handle"]))

def parse_row(meta, row_index, col_coords):
#    if col_coords == None:
#        col_coords = [[0, 0, meta["ll"]]]

    return b"".join(parse_data_values(row_index, meta["ll"], col_coords, meta["data_handle"]))

#def parse_row_values(meta, row_index, col_coords=None):
#    if col_coords == None:
#        col_coords = meta["col_coords"]
#
#    return [x for x in parse_data_values(row_index, meta["ll"], col_coords, meta["data_handle"])]

def parse_row_for_sample(meta, sample_id, col_coords):
    if sample_id in meta["sample_lookup"]:
        return parse_row(meta, meta["sample_lookup"][sample_id], col_coords)
    else:
        # The last column has a newline character at the end, so we do this (as a hack) to ignore it.
        col_coords2 = list(col_coords)
        col_coords2[-1][-1] -= 1

        return b"".join([b" " * (coords[2] - coords[1]) for coords in col_coords2])

#def parse_row_values_for_sample(meta, sample_id, col_coords=None):
#    return parse_row_values(meta, meta["sample_lookup"][sample_id], col_coords)

def parse_meta_value(meta, col_index, length_key, handle_key):
    return next(parse_data_values(col_index, meta[length_key] + 1, [(col_index, 0, meta[length_key])], meta[handle_key]))

#def parse_col_length(meta, col_index):
#    return parse_meta_value(meta, col_index, "mccl", "cc_handle")

def parse_col_name(meta, col_index):
    return parse_meta_value(meta, col_index, "mcnl", "cn_handle")

def parse_col_type(meta, col_index):
    return parse_meta_value(meta, col_index, "mctl", "ct_handle")

def merge_fwf_files(in_file_paths, out_file_path):
    in_file_paths = sorted(in_file_paths)

    # Open files for reading and pull metadata
    in_file_meta = {}
    for in_file_path in in_file_paths:
        meta = {}
        meta["data_num_rows"] = countFileLines(in_file_path)
        meta["data_num_cols"] = countFileLines(in_file_path, ".cc")
        meta["data_handle"] = openReadFile(in_file_path)

        meta["ll"] = readIntFromFile(in_file_path, ".ll")
        meta["cc_handle"] = openReadFile(in_file_path, ".cc")
        meta["mccl"] = readIntFromFile(in_file_path, ".mccl")
        meta["col_coords"] = list(parse_data_coords(range(meta["data_num_cols"]), meta["cc_handle"], meta["mccl"], meta["ll"]))
        meta["cn_handle"] = openReadFile(in_file_path, ".cn")
        meta["mcnl"] = readIntFromFile(in_file_path, ".mcnl")
        meta["ct_handle"] = openReadFile(in_file_path, ".ct")
        meta["mctl"] = readIntFromFile(in_file_path, ".mctl")

#        meta["variables"] = parse_row_values(meta, 0)
#        meta["variable_lookup"] = {}
#        for i, variable in zip(range(meta["data_num_cols"]), meta["variables"]):
#            meta["variable_lookup"][variable.rstrip()] = i

        meta["sample_lookup"] = {}
        for i, sample_id in zip(range(meta["data_num_rows"]), parse_column_values(meta, 0, 0)):
            meta["sample_lookup"][sample_id] = i

        in_file_meta[in_file_path] = meta

    # Find unique sample IDs
    all_sample_ids = set()
    for in_file_path in in_file_paths:
        meta = in_file_meta[in_file_path]
        for sample_id in meta["sample_lookup"]:
            all_sample_ids.add(sample_id)
    all_sample_ids = sorted(list(all_sample_ids))

    # The widths of the Sample columns could be different in different files,
    #   so we need to deal with that. Check the meta values and find the longest.
    longest_sample_id = getMaxStringLength([x.decode() for x in all_sample_ids])

    # Calculate the column start coordinates for the merged data
    column_start_coords = [0]
    cumulative_position = longest_sample_id

    for in_file_path in in_file_paths:
        col_coords = in_file_meta[in_file_path]["col_coords"]

        for i in range(1, len(col_coords)):
            column_start_coords.append(str(cumulative_position))
            column_size = col_coords[i][2] - col_coords[i][1]
            cumulative_position += column_size

    column_coords_string, max_column_coord_length = buildStringMap(column_start_coords)
    writeStringToFile(out_file_path, ".cc", column_coords_string)
    writeStringToFile(out_file_path, ".mccl", max_column_coord_length)

    # Merge column names
    column_names = [format_string(b"Sample", longest_sample_id).decode()]
    for in_file_path in in_file_paths:
        for col_index in range(1, in_file_meta[in_file_path]["data_num_cols"]):
            column_name = parse_col_name(in_file_meta[in_file_path], col_index).rstrip().decode()

            in_file_extension = os.path.splitext(in_file_path)[1]
            prefix = os.path.basename(in_file_path).replace(in_file_extension, "")

            column_names.append("{}__{}".format(prefix, column_name))

    # Save column names to file
    column_names_string, max_column_names_length = buildStringMap(column_names)
    writeStringToFile(out_file_path, ".cn", column_names_string)
    writeStringToFile(out_file_path, ".mcnl", max_column_names_length)

    # Calculate the column types for the merged data
    column_types = [get_most_generic_type(all_sample_ids)]
    for in_file_path in in_file_paths:
        for col_index in range(1, in_file_meta[in_file_path]["data_num_cols"]):
            column_types.append(parse_col_type(in_file_meta[in_file_path], col_index).decode())

    # Save column types to file
    column_types_string, max_column_types_length = buildStringMap(column_types)
    writeStringToFile(out_file_path, ".ct", column_types_string)
    writeStringToFile(out_file_path, ".mctl", max_column_types_length)

    # Output the merged data values and meta values
    with open(out_file_path, 'wb') as out_file:
        for i, sample_id in enumerate(all_sample_ids):
            out_line = format_string(sample_id, longest_sample_id)

            for in_file_path in in_file_paths:
                meta = in_file_meta[in_file_path]
                out_line += parse_row_for_sample(meta, sample_id, meta["col_coords"][1:]).rstrip(b"\n")

            out_file.write(out_line + b"\n")

            if i == 0:
                writeStringToFile(out_file_path, ".ll", str(len(out_line) + 1).encode())

    for meta in in_file_meta.values():
        meta["data_handle"].close()
        meta["cc_handle"].close()
        meta["cn_handle"].close()
        meta["ct_handle"].close()

def query_fwf_file(in_file_path, out_file_path):
    meta = {}
#    meta["data_num_rows"] = countFileLines(in_file_path)
#    meta["data_num_cols"] = countFileLines(in_file_path, ".cc")
    meta["data_handle"] = openReadFile(in_file_path)

    meta["ll"] = readIntFromFile(in_file_path, ".ll")
    meta["cc_handle"] = openReadFile(in_file_path, ".cc")
    meta["mccl"] = readIntFromFile(in_file_path, ".mccl")
#    meta["col_coords"] = list(parse_data_coords(range(meta["data_num_cols"]), meta["cc_handle"], meta["mccl"], meta["ll"]))
#    meta["cn_handle"] = openReadFile(in_file_path, ".cn")
#    meta["mcnl"] = readIntFromFile(in_file_path, ".mcnl")
#    meta["ct_handle"] = openReadFile(in_file_path, ".ct")
#    meta["mctl"] = readIntFromFile(in_file_path, ".mctl")

#        meta["variables"] = parse_row_values(meta, 0)
#        meta["variable_lookup"] = {}
#        for i, variable in zip(range(meta["data_num_cols"]), meta["variables"]):
#            meta["variable_lookup"][variable.rstrip()] = i

#    meta["sample_lookup"] = {}
#    for i, sample_id in zip(range(meta["data_num_rows"]), parse_column_values(meta, 0, 0)):
#        meta["sample_lookup"][sample_id] = i

    out_col_indices = [2, 5]
    out_col_coords = list(parse_data_coords(out_col_indices, meta["cc_handle"], meta["mccl"], meta["ll"]))

    with open(out_file_path, 'wb') as out_file:
        all_query_col_coords = parse_data_coords(query_col_indices, meta["cc_handle"], meta["mccl"], meta["ll"])
        keep_row_indices = range(1, num_rows)

        for query_col_index in query_col_indices:
            keep_row_indices = filter_rows(keep_row_indices, query_col_index, [next(all_query_col_coords)])

        chunk_size = 1000
        out_lines = []

        for row_index in [0] + list(keep_row_indices):
            out_lines.append(b"\t".join(parse_data_values(row_index, line_length, out_col_coords, file_handles["data"])).rstrip())

            if len(out_lines) % chunk_size == 0:
                out_file.write(b"\n".join(out_lines) + b"\n")
                out_lines = []

        if len(out_lines) > 0:
            out_file.write(b"\n".join(out_lines) + b"\n")

    meta["data_handle"].close()
#    meta["cc_handle"].close()
#    meta["cn_handle"].close()
#    meta["ct_handle"].close()
