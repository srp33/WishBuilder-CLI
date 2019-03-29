import fastnumbers
import json
import mmap
import os
import sys
import time
from DataSetHelper import *

def convert_tsv_to_fwf(file_path, out_file_path):
    column_size_dict = {}
    column_start_coords = []

    # Initialize a dictionary with the column index as key and width of the column as value
    with open(file_path, 'rb') as my_file:
        col_names = my_file.readline().rstrip(b"\n").split(b"\t")

        for i in range(len(col_names)):
            column_size_dict[i] = 0

    # Iterate through the lines to find the max width of each column
    with open(file_path, 'rb') as my_file:
        # Ignore the header line because we saved column names elsewhere
        my_file.readline()

        num_rows = 0
        for line in my_file:
            num_rows += 1
            line_items = line.rstrip(b"\n").split(b"\t")

            for i in range(len(line_items)):
                column_size_dict[i] = max([column_size_dict[i], len(line_items[i])])

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
    column_start_coords.append(str(cumulative_position))

    # Build a map of the column names and save this to a file
    col_names_string, max_col_name_length = buildStringMap([x.decode() for x in col_names])
    writeStringToFile(out_file_path, ".cn", col_names_string)
    writeStringToFile(out_file_path, ".mcnl", max_col_name_length)

    # Calculate the column coordinates and max length of these coordinates
    column_coords_string, max_column_coord_length = buildStringMap(column_start_coords)

    # Save column coordinates
    writeStringToFile(out_file_path, ".cc", column_coords_string)

    # Save value that indicates maximum length of column coords string
    writeStringToFile(out_file_path, ".mccl", max_column_coord_length)

    # Save number of rows and cols
    writeStringToFile(out_file_path, ".nrow", str(num_rows).encode())
    writeStringToFile(out_file_path, ".ncol", str(len(col_names)).encode())

    # Save the data to output file
    with open(file_path, 'rb') as my_file:
        # Ignore the header line because we saved column names elsewhere
        my_file.readline()

        with open(out_file_path, 'wb') as out_file:
            num_rows = 0
            for line in my_file:
                num_rows += 1
                line_items = line.rstrip(b"\n").split(b"\t")

                line_out = b""
                for i in sorted(column_size_dict.keys()):
                    line_out += format_string(line_items[i], column_size_dict[i])

                # This newline character is unnecessary, so it adds a bit of disk space.
                # However, it makes the files much more readable to humans.
                out_file.write(line_out + b"\n")

    parse_and_save_column_types(out_file_path)

def parse_and_save_column_types(file_path):
    # Initialize
    data_handle = openReadFile(file_path)
    ll = readIntFromFile(file_path, ".ll")
    cc_handle = openReadFile(file_path, ".cc")
    mccl = readIntFromFile(file_path, ".mccl")
    cn_handle = openReadFile(file_path, ".cn")
    mcnl = readIntFromFile(file_path, ".mcnl")
    num_rows = readIntFromFile(file_path, ".nrow")
    num_cols = readIntFromFile(file_path, ".ncol")
    col_coords = list(parse_data_coords(range(num_cols), cc_handle, mccl))

    # Find column type and description for each column
    column_types = []
    column_descriptions = []
    for col_index in range(num_cols):
        column_name = parse_meta_value(cn_handle, mcnl, col_index)
        column_values = [x.rstrip() for x in parse_column_values(data_handle, num_rows, col_coords, ll, 0, col_index)]

        if column_name.rstrip() == b"Sample":
            column_type = "i"
        else:
            column_type = parse_column_type(column_values)

        column_types.append(column_type)
        column_descriptions.append(get_column_description(column_type, column_values))

    # Save the column types and max length of these types
    column_types_string, max_column_types_length = buildStringMap(column_types)
    writeStringToFile(file_path, ".ct", column_types_string)
    writeStringToFile(file_path, ".mctl", max_column_types_length)

    # Save column type descriptions and max length of these
    column_desc_string, max_column_desc_length = buildStringMap(column_descriptions)
    writeStringToFile(file_path, ".cd", column_desc_string)
    writeStringToFile(file_path, ".mcdl", max_column_desc_length)

    data_handle.close()
    cc_handle.close()
    cn_handle.close()

def parse_column_type(values):
    unique_values = set(values)

    for x in unique_values:
        if not fastnumbers.isfloat(x):
            return "d" # Discrete

    return "n" # Numeric

def get_column_description(column_type, column_values):
    if column_type == "i":
        meta_dict = {"options": ["ID"], "numOptions": len(column_values)}
    elif column_type == "n":
        float_values = [float(x) for x in column_values if len(x) > 0]
        meta_dict = {"options": "continuous", "min": min(float_values), "max": max(float_values)}
    else:
        if len(column_values) == 0:
            meta_dict = {"options": ["NA"], "numOptions": 1}
        else:
            unique_values = sorted([x.decode() for x in set(column_values)])
            meta_dict = {"options": unique_values, "numOptions": len(unique_values)}

    return json.dumps(meta_dict)

def format_string(x, size):
    formatted = "{:<" + str(size) + "}"
    return formatted.format(x.decode()).encode()

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

def writeStringToFile(file_path, file_extension, the_string):
    with open(file_path + file_extension, 'wb') as the_file:
        the_file.write(the_string)

def countFileLines(file_path, file_extension=""):
    num_lines = 0

    with open(file_path + file_extension, 'rb') as the_file:
        for line in the_file:
            num_lines += 1

    return num_lines

def parse_column_values(data_handle, data_num_rows, cc, ll, row_start_index, col_index):
    col_coords = [cc[col_index]]
    for row_index in range(row_start_index, data_num_rows):
        yield next(parse_data_values(row_index, ll, col_coords, data_handle))

def parse_row(meta, row_index, col_coords):
    return b"".join(parse_data_values(row_index, meta["ll"], col_coords, meta["data_handle"]))

def parse_row_for_sample(meta, sample_id, col_coords):
    if sample_id in meta["sample_lookup"]:
        return parse_row(meta, meta["sample_lookup"][sample_id], col_coords)
    else:
        # Fill in missing data points with spaces
        return b"".join([b" " * (coords[2] - coords[1]) for coords in col_coords])

def merge_fwf_files(in_file_paths, out_file_path):
    in_file_paths = sorted(in_file_paths)

    # Open files for reading and pull metadata
    in_file_meta = {}
    for in_file_path in in_file_paths:
        meta = {}
        meta["data_num_rows"] = readIntFromFile(in_file_path, ".nrow")
        meta["data_num_cols"] = readIntFromFile(in_file_path, ".ncol")
        meta["data_handle"] = openReadFile(in_file_path)
        meta["ll"] = readIntFromFile(in_file_path, ".ll")
        meta["cc_handle"] = openReadFile(in_file_path, ".cc")
        meta["mccl"] = readIntFromFile(in_file_path, ".mccl")
        meta["col_coords"] = list(parse_data_coords(range(meta["data_num_cols"]), meta["cc_handle"], meta["mccl"]))
        meta["cn_handle"] = openReadFile(in_file_path, ".cn")
        meta["mcnl"] = readIntFromFile(in_file_path, ".mcnl")
        meta["ct_handle"] = openReadFile(in_file_path, ".ct")
        meta["mctl"] = readIntFromFile(in_file_path, ".mctl")
        meta["cd_handle"] = openReadFile(in_file_path, ".cd")
        meta["mcdl"] = readIntFromFile(in_file_path, ".mcdl")

        meta["sample_lookup"] = {}
        for i, sample_id in zip(range(meta["data_num_rows"]), parse_column_values(meta["data_handle"], meta["data_num_rows"], meta["col_coords"], meta["ll"], 0, 0)):
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
    column_start_coords = ["0"]
    cumulative_position = longest_sample_id

    for in_file_path in in_file_paths:
        col_coords = in_file_meta[in_file_path]["col_coords"]

        for i in range(1, len(col_coords)):
            column_size = col_coords[i][2] - col_coords[i][1]
            column_start_coords.append(str(cumulative_position))
            cumulative_position += column_size

    column_start_coords.append(str(cumulative_position))
    column_coords_string, max_column_coord_length = buildStringMap(column_start_coords)
    writeStringToFile(out_file_path, ".cc", column_coords_string)
    writeStringToFile(out_file_path, ".mccl", max_column_coord_length)

    # Merge column names
    column_names = [format_string(b"Sample", longest_sample_id).decode()]
    group_dict = {}
    for in_file_path in in_file_paths:
        for col_index in range(1, in_file_meta[in_file_path]["data_num_cols"]):
            column_name = parse_meta_value(in_file_meta[in_file_path]["cn_handle"], in_file_meta[in_file_path]["mcnl"], col_index).rstrip().decode()

            in_file_extension = os.path.splitext(in_file_path)[1]
            prefix = os.path.basename(in_file_path).replace(in_file_extension, "")

            column_name = "{}__{}".format(prefix, column_name)
            column_names.append(column_name)

            if prefix not in group_dict:
                group_dict[prefix] = []
            group_dict[prefix].append(column_name)

    # Save column names to file
    column_names_string, max_column_names_length = buildStringMap(column_names)
    writeStringToFile(out_file_path, ".cn", column_names_string)
    writeStringToFile(out_file_path, ".mcnl", max_column_names_length)

    # Save groups to a file
    if len(group_dict) > 1:
        with open(out_file_path + ".groups", "wb") as group_file:
            for group_name, column_names in group_dict.items():
                group_file.write("{}\t{}\n".format(group_name, "\t".join(column_names)).encode())

    # Calculate the column types and descriptions for the merged data
    column_types = ["i"] # This is the Sample column
    column_descriptions = [parse_meta_value(in_file_meta[in_file_path]["cd_handle"], in_file_meta[in_file_path]["mcdl"], col_index).decode()]
    for in_file_path in in_file_paths:
        for col_index in range(1, in_file_meta[in_file_path]["data_num_cols"]):
            column_types.append(parse_meta_value(in_file_meta[in_file_path]["ct_handle"], in_file_meta[in_file_path]["mctl"], col_index).decode())
            column_descriptions.append(parse_meta_value(in_file_meta[in_file_path]["cd_handle"], in_file_meta[in_file_path]["mcdl"], col_index).decode())

    # Save column types to file
    column_types_string, max_column_types_length = buildStringMap(column_types)
    writeStringToFile(out_file_path, ".ct", column_types_string)
    writeStringToFile(out_file_path, ".mctl", max_column_types_length)

    # Save column descriptions to file
    column_desc_string, max_column_desc_length = buildStringMap(column_descriptions)
    writeStringToFile(out_file_path, ".cd", column_desc_string)
    writeStringToFile(out_file_path, ".mcdl", max_column_desc_length)

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

    # Save num rows and cols
    writeStringToFile(out_file_path, ".nrow", str(len(all_sample_ids)).encode())
    writeStringToFile(out_file_path, ".ncol", str(len(column_types)).encode())

    for meta in in_file_meta.values():
        for key, value in meta.items():
            if key.endswith("_handle"):
                value.close()

def parse_yaml_entry(yaml_file, entry):
    # We'll just parse the file manually, don't need YAML parser for this.
    value = yaml_file.readline().rstrip("\n").replace("{}: ".format(entry), "").strip().encode()

    if len(value) == 0:
        raise Exception("The {} was empty in {}.".format(entry, yaml_file_path))

    return value

def build_metadata(dataset_id, md_file_path, yaml_file_path, data_file_path):
    with open(md_file_path) as md_file:
        writeStringToFile(data_file_path, ".description", md_file.read().strip().encode())

    with open(yaml_file_path) as yaml_file:
        for entry in ("title", "featureDescription", "featureDescriptionPlural"):
            writeStringToFile(data_file_path, "." + entry, parse_yaml_entry(yaml_file, entry))

    writeStringToFile(data_file_path, ".id", dataset_id.encode())
    writeStringToFile(data_file_path, ".timestamp", str(time.time()).encode())
