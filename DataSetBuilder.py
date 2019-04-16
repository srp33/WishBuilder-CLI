import gzip
import fastnumbers
import mmap
import os
import shutil
import sys
import time
from DataSetHelper import *

def convert_tsv_to_fwf(tsv_file_path, fwf_file_path):
    column_size_dict = {}
    column_start_coords = []

    # Initialize a dictionary with the column index as key and width of the column as value
    with open(tsv_file_path, 'rb') as my_file:
        column_names = my_file.readline().rstrip(b"\n").split(b"\t")

        for i in range(len(column_names)):
            column_size_dict[i] = 0

    # Iterate through the lines to find the max width of each column
    with open(tsv_file_path, 'rb') as my_file:
        # Ignore the header line because we saved column names elsewhere
        my_file.readline()

        num_rows = 0
        for line in my_file:
            num_rows += 1
            line_items = line.rstrip(b"\n").split(b"\t")

            for i in range(len(line_items)):
                column_size_dict[i] = max([column_size_dict[i], len(line_items[i])])

    # Calculate the length of the first line (and thus all the other lines)
    line_length = sum([column_size_dict[i] for i in range(len(column_names))])

    # Save value that indicates line length
    writeStringToFile(fwf_file_path, ".ll", str(line_length + 1).encode())

    # Calculate the positions where each column starts
    cumulative_position = 0
    for i in range(len(column_names)):
        column_size = column_size_dict[i]
        column_start_coords.append(str(cumulative_position).encode())
        cumulative_position += column_size
    column_start_coords.append(str(cumulative_position).encode())

    # Build a map of the column names and save this to a file
    column_names_string, max_col_name_length = buildStringMap([x for x in column_names])
    writeStringToFile(fwf_file_path, ".cn", column_names_string)
    writeStringToFile(fwf_file_path, ".mcnl", max_col_name_length)

    # Calculate the column coordinates and max length of these coordinates
    column_coords_string, max_column_coord_length = buildStringMap(column_start_coords)

    # Save column coordinates
    writeStringToFile(fwf_file_path, ".cc", column_coords_string)

    # Save value that indicates maximum length of column coords string
    writeStringToFile(fwf_file_path, ".mccl", max_column_coord_length)

    # Save number of rows and cols
    writeStringToFile(fwf_file_path, ".nrow", str(num_rows).encode())
    writeStringToFile(fwf_file_path, ".ncol", str(len(column_names)).encode())

    # Save the data to output file
    with open(tsv_file_path, 'rb') as my_file:
        # Ignore the header line because we saved column names elsewhere
        my_file.readline()

        with open(fwf_file_path, 'wb') as out_file:
            out_lines = []
            chunk_size = 1000

            for line in my_file:
                line_items = line.rstrip(b"\n").split(b"\t")

                line_out = b""
                for i in sorted(column_size_dict.keys()):
                    line_out += format_string(line_items[i], column_size_dict[i])

                out_lines.append(line_out)

                if len(out_lines) % chunk_size == 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
                    out_lines = []

            if len(out_lines) > 0:
                out_file.write(b"\n".join(out_lines) + b"\n")

    parse_and_save_column_types(fwf_file_path)

    # Save group names and indices to file
    in_file_extension = os.path.splitext(tsv_file_path)[1]
    group_name = os.path.basename(tsv_file_path).replace(in_file_extension, "").encode()
    group_indices_dict = {group_name: list(range(1, len(column_names)))}
    group_dict = {group_name: column_names[1:]}
    save_column_index_map_to_file(fwf_file_path, ".groups", group_indices_dict, group_dict)

    # Save pathway information
    alias_dict = build_alias_dict(tsv_file_path)
    pathway_gene_indices_dict = map_pathway_dict_to_column_indices(column_names, alias_dict)
    if len(pathway_gene_indices_dict) > 0:
        save_column_index_map_to_file(fwf_file_path, ".pathways", pathway_gene_indices_dict)

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
        column_type = parse_column_type(column_name, column_values)

        if col_index % 100 == 0:
            print("Finding column type and description - {}".format(col_index))

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

def parse_column_type(name, values):
    if name == b"Sample":
        return b"i"

    non_missing_values = [x for x in values if x != b"" and x != b"NA"]
    unique_values = set(non_missing_values)

    has_non_number = False
    for x in unique_values:
        if not fastnumbers.isfloat(x):
            has_non_number = True
            break

    if has_non_number:
        if len(unique_values) == len(non_missing_values):
            return b"i" #ID
        else:
            return b"d" #Discrete

    return b"n" # Numeric

def get_column_description(column_type, column_values):
    if column_type == b"i":
        return "{}|ID".format(len(column_values)).encode() # It doesn't make sense to store all the IDs in the description file.

    non_missing_values = [x for x in column_values if x != b"" and x != b"NA"]

    if len(non_missing_values) == 0:
        return "1|NA".encode()

    if column_type == b"n":
        float_values = [float(x) for x in non_missing_values]
        return "{:.8f},{:.8f}".format(min(float_values), max(float_values)).encode()

    # Discrete
    unique_values = sorted([x.decode() for x in set(non_missing_values)])
    return "{}|{}".format(len(unique_values), ",".join(unique_values)).encode()

def format_string(x, size):
    formatted = "{:<" + str(size) + "}"
    return formatted.format(x.decode()).encode()

def getMaxStringLength(the_list):
    return max([len(x) for x in set(the_list)])

def buildStringMap(the_list):
    # Find maximum length of value
    max_value_length = getMaxStringLength(the_list)

    # Build output string
    output = ""
    formatter = "{:<" + str(max_value_length) + "}\n"
    for value in the_list:
        output += formatter.format(value.decode())

    return output.encode(), str(max_value_length).encode()

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

def build_gene_pathways_dict():
    in_gmt_file_url = "https://www.pathwaycommons.org/archives/PC2/v11/PathwayCommons11.All.hgnc.gmt.gz"
    in_gmt_file_path = "/tmp/{}".format(os.path.basename(in_gmt_file_url))
    if not os.path.exists(in_gmt_file_path):
        os.system("wget -O {} {}".format(in_gmt_file_path, in_gmt_file_url))

    gene_pathways_dict = {}
    pathways = set()

    with gzip.open(in_gmt_file_path, 'rb') as in_gmt_file:
        for line in in_gmt_file:
            line_items = line.rstrip(b"\n").split(b"\t")

            data_source = line_items[1].split(b";")[1].replace(b"datasource: ", b"").strip()
            pathway_name = line_items[1].split(b";")[0].replace(b"name: ", b"").strip() + b" [" + data_source + b"]"
            genes = line_items[2:]

            # I don't think it makes sense to call something a pathway if there is only one gene
            if len(genes) < 2:
                continue

            for gene in genes:
                if gene not in gene_pathways_dict:
                    gene_pathways_dict[gene] = set()
                gene_pathways_dict[gene].add(pathway_name)

                pathways.add(pathway_name)

    return gene_pathways_dict, pathways

def parse_column_names(fwf_file_path):
    with open(fwf_file_path + ".cn", "rb") as cn_file:
        return [x.rstrip() for x in cn_file]

def map_pathway_dict_to_column_indices(column_names, alias_dict):
    gene_pathways_dict, pathways = build_gene_pathways_dict()
    pathway_gene_indices_dict = {pathway:set() for pathway in pathways}

    for i, column_name in enumerate(column_names):
        alias = alias_dict.get(column_name, "")

        if column_name in gene_pathways_dict:
            for pathway in gene_pathways_dict[column_name]:
                pathway_gene_indices_dict[pathway].add(i)
        elif alias in gene_pathways_dict:
            for pathway in gene_pathways_dict[alias]:
                pathway_gene_indices_dict[pathway].add(i)

    pathway_gene_indices_dict2 = {}
    for pathway, gene_indices in pathway_gene_indices_dict.items():
        if len(gene_indices) > 0:
            pathway_gene_indices_dict2[pathway] = sorted(list(pathway_gene_indices_dict[pathway]))

    return pathway_gene_indices_dict2

def map_column_name_dict_to_indices(the_dict, column_names):
    map_dict = {}

    for name, genes in the_dict.items():
        overlapping_genes = set(column_names) & set(genes)

        if len(overlapping_genes) > 0:
            overlapping_gene_indices = [column_names.index(gene) for gene in overlapping_genes]
            overlapping_gene_indices = [x for x in sorted(overlapping_gene_indices)]

            map_dict[name] = overlapping_gene_indices

    return map_dict

def save_column_index_map_to_file(fwf_file_path, file_extension, index_dict, value_dict=None):
    output = b""

    for name, indices in sorted(index_dict.items()):
        values = ""
        if value_dict and name in value_dict and len(value_dict[name]) > 0:
            values = ",".join([x.decode() for x in value_dict[name]])

        if values == "":
            output += "{}\t{}\n".format(name.decode(), ",".join([str(i) for i in indices])).encode()
        else:
            output += "{}\t{}\t{}\n".format(name.decode(), ",".join([str(i) for i in indices]), values).encode()

    if len(output) > 0:
        writeStringToFile(fwf_file_path, file_extension, output)

def build_alias_dict(tsv_file_path):
    aliases_file_path = tsv_file_path + ".aliases"
    alias_dict = {}

    if os.path.exists(aliases_file_path):
        with open(aliases_file_path, 'rb') as aliases_file:
            for line in aliases_file:
                line_items = line.rstrip(b"\n").split(b"\t")
                alias_dict[line_items[0]] = line_items[1]

    return alias_dict

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
    longest_sample_id = getMaxStringLength([x for x in all_sample_ids])

    # Calculate the column start coordinates for the merged data
    column_start_coords = [b"0"]
    cumulative_position = longest_sample_id

    for in_file_path in in_file_paths:
        col_coords = in_file_meta[in_file_path]["col_coords"]

        for i in range(1, len(col_coords)):
            column_size = col_coords[i][2] - col_coords[i][1]
            column_start_coords.append(str(cumulative_position).encode())
            cumulative_position += column_size

    column_start_coords.append(str(cumulative_position).encode())
    column_coords_string, max_column_coord_length = buildStringMap(column_start_coords)
    writeStringToFile(out_file_path, ".cc", column_coords_string)
    writeStringToFile(out_file_path, ".mccl", max_column_coord_length)

    # Merge column names and pathway information
    original_column_names = [b"Sample"]
    merged_column_names = [b"Sample"]
    merged_pathway_gene_dict = {}
    group_dict = {}

    for in_file_path in in_file_paths:
        pathway_gene_indices_dict = {}
        pathways_file_path = in_file_path + ".pathways"
        if os.path.exists(pathways_file_path):
            with open(pathways_file_path, 'rb') as pathways_file:
                for line in pathways_file:
                    line_items = line.rstrip(b"\n").split(b"\t")
                    pathway_gene_indices_dict[line_items[0]] = [int(x) for x in line_items[1].split(b",")]

        for col_index in range(1, in_file_meta[in_file_path]["data_num_cols"]):
            column_name = parse_meta_value(in_file_meta[in_file_path]["cn_handle"], in_file_meta[in_file_path]["mcnl"], col_index).rstrip()
            original_column_names.append(column_name)

            for pathway_name, gene_indices in pathway_gene_indices_dict.items():
                if col_index in gene_indices:
                    merged_pathway_gene_dict[pathway_name] = merged_pathway_gene_dict.setdefault(pathway_name, []) + [column_name]

            in_file_extension = os.path.splitext(in_file_path)[1]
            prefix = os.path.basename(in_file_path).replace(in_file_extension, "").encode()

            merged_column_name = "{}__{}".format(prefix.decode(), column_name.decode()).encode()
            merged_column_names.append(merged_column_name)

            if prefix not in group_dict:
                group_dict[prefix] = []
            group_dict[prefix].append(column_name)

    # Save merged column names to file
    column_names_string, max_column_names_length = buildStringMap(merged_column_names)
    writeStringToFile(out_file_path, ".cn", column_names_string)
    writeStringToFile(out_file_path, ".mcnl", max_column_names_length)

    # Save pathway gene indices to file
    pathway_gene_indices_dict = map_column_name_dict_to_indices(merged_pathway_gene_dict, original_column_names)
    save_column_index_map_to_file(out_file_path, ".pathways", pathway_gene_indices_dict)

    # Save group names and indices to file
    group_indices_dict = map_column_name_dict_to_indices(group_dict, original_column_names)
    save_column_index_map_to_file(out_file_path, ".groups", group_indices_dict, group_dict)

    # Calculate the column types and descriptions for the merged data
    column_types = [b"i"] # This is the Sample column
    column_descriptions = [get_column_description(b"i", all_sample_ids)]

    for in_file_path in in_file_paths:
        for col_index in range(1, in_file_meta[in_file_path]["data_num_cols"]):
            column_types.append(parse_meta_value(in_file_meta[in_file_path]["ct_handle"], in_file_meta[in_file_path]["mctl"], col_index))
            column_descriptions.append(parse_meta_value(in_file_meta[in_file_path]["cd_handle"], in_file_meta[in_file_path]["mcdl"], col_index))

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
        chunk_size = 1000
        out_lines = []
        for i, sample_id in enumerate(all_sample_ids):
            out_line = format_string(sample_id, longest_sample_id)

            for in_file_path in in_file_paths:
                meta = in_file_meta[in_file_path]
                out_line += parse_row_for_sample(meta, sample_id, meta["col_coords"][1:]).rstrip(b"\n")

            out_lines.append(out_line)

            if len(out_lines) % chunk_size == 0:
                out_file.write(b"\n".join(out_lines) + b"\n")
                out_lines = []

            if i == 0:
                writeStringToFile(out_file_path, ".ll", str(len(out_line) + 1).encode())

        if len(out_lines) > 0:
            out_file.write(b"\n".join(out_lines) + b"\n")

    # Save num rows and cols
    writeStringToFile(out_file_path, ".nrow", str(len(all_sample_ids)).encode())
    writeStringToFile(out_file_path, ".ncol", str(len(column_types)).encode())

    for meta in in_file_meta.values():
        for key, value in meta.items():
            if key.endswith("_handle"):
                value.close()

def parse_yaml_entry(yaml_file, entry):
    # We'll just parse the file manually, don't need YAML parser for this.
    value = yaml_file.readline().decode().rstrip("\n").replace("{}: ".format(entry), "").strip().encode()

    if len(value) == 0:
        raise Exception("The {} was empty in {}.".format(entry, yaml_file_path))

    return value

def build_metadata(data_dir_path, fwf_file_path):
    dataset_id = os.path.basename(data_dir_path)

    md_file_path = data_dir_path + "/description.md"
    yaml_file_path = data_dir_path + "/config.yaml"

    with open(md_file_path, 'rb') as md_file:
        writeStringToFile(fwf_file_path, ".description", md_file.read().strip())

    with open(yaml_file_path, 'rb') as yaml_file:
        for entry in ("title"):
            writeStringToFile(fwf_file_path, "." + entry, parse_yaml_entry(yaml_file, entry))

    writeStringToFile(fwf_file_path, ".id", dataset_id.encode())
    writeStringToFile(fwf_file_path, ".timestamp", str(time.time()).encode())
