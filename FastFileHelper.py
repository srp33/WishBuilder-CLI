import os
import datetime
import math
import msgpack
import shutil
import sys
#import gzip as gz
#import indexed_gzip as igz
#import subprocess

def smart_print(x):
    """Print with current date and time

    Args:
        x: output to be printed
    """
    print("{} - {}".format(datetime.datetime.now(), x))
    sys.stdout.flush()

def reset_dict(my_dict):
    """Simple function to reset values of dictionary

    Args:
        my_dict (dict): Dictionary to be reset

    Dictionary must have keys already
    """

    for k in my_dict:
        my_dict[k] = []

    return my_dict

def calculate_chunks(nrows, ncols, n_data_points=500000000):
    """Dynamically calculate chunk size

    Args:
        nrows (int): Number of rows in tsv file
        ncols (int): Number of columns in tsv file
        n_data_points (int): About how many data points per chunk
    """

    chunk_size = math.floor(n_data_points / ncols)
    if chunk_size > nrows:
        chunk_size = math.floor(nrows / 2)
    n_chunks = nrows / chunk_size
    return chunk_size, n_chunks

def dynamic_open(file_path, is_gzipped=False, mode='r', index_file=None, use_gzip_module=False):
    """Dynamically open a file

    Args:
        file_path (str): Path to file
        is_gzipped (bool): Is the file gzipped
        mode (str): 'r', 'w', blah blah
        index_file (str): Pass this bad boi into the index file slot
        use_gzip_module (bool): Override indexed_gzip for when we don't need an index file
    """
    if use_gzip_module:
        return gz.open(filename=file_path, mode=mode)
    elif is_gzipped:
        return igz.IndexedGzipFile(filename=file_path, mode=mode, index_file=index_file)
    else:
        return open(file_path, mode)

def open_msgpack(file_path, mode, store_data=None):
    """Access msgpack

    Args:
        file_path (str): Path to msgpack file, either to be created or already existent
        mode (str): Either 'rb' or 'wb' (file must be read as bytes)
        store_data (dict or list): Object to store if mode == 'wb'

    Returns:
        Dictionary created from unpack or whatever you get from packing
    """
    with open(file_path, mode) as cur_file:
        if mode == 'rb':
            return msgpack.unpack(cur_file)
        elif mode == 'wb':
            if store_data is None:
                smart_print("\033[91mWARNING:\033[0m No data given to pack")
            return msgpack.pack(store_data, cur_file)
        else:
            raise ValueError("`mode` must be either 'rb' or 'wb'")

def map_tsv(in_file_path, output_dir, is_gzipped=False):
    """Map TSV using dictionary to be saved to MessagePack to hold position in file and length of data for sample

    Args:
          in_file_path (str): file path for input
          output_dir (str): path for database to be created
          is_gzipped (bool): is the input file gzipped?
    """
    samples = []
    example_samples = []

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    sample_data = {}

    #################
    # ITERATE THROUGH
    #     FILE
    #################

    with dynamic_open(in_file_path, is_gzipped, 'r') as in_file:
        if is_gzipped:
            in_file.build_full_index()
            in_file.export_index('/'.join([output_dir, 'indices.gzidx']))

            feature_names = in_file.readline().decode().rstrip().split('\t')
        else:
            feature_names = in_file.readline().rstrip().split('\t')

        sample_ix = feature_names.index("Sample")

        # Get positioning after header (tell() cannot be called when using an iterator)
        cur_pos = in_file.tell()
        for i, line in enumerate(in_file):
            if i > 0 and i % 50000 == 0:
                smart_print(i)

            cur_len = len(line)

            if is_gzipped:
                line = line.decode()

            data_list = line.rstrip().split('\t')
            sample = data_list[sample_ix]
            samples.append(sample)

            # Adding '\t' at the beginning ensures we get the correct string, however, we must add one to the index
            #   in order to get the correct positioning and not include the '\t'
            data_starter = '\t' + data_list[sample_ix + 1]
            cur_line_ix = line.index(data_starter) + 1
            start_ix = cur_line_ix + cur_pos

            data_length = len(line.rstrip('\n').encode('utf-8')) - cur_line_ix

            sample_data[sample] = (start_ix, data_length)

            cur_pos += cur_len

    in_file = dynamic_open(in_file_path, is_gzipped, 'r')
    in_file.close()

    open_msgpack('/'.join([output_dir, 'sample_data.msgpack']), 'wb', sample_data)
    open_msgpack('/'.join([output_dir, 'samples.msgpack']), 'wb', samples)
    open_msgpack('/'.join([output_dir, 'features.msgpack']), 'wb', feature_names)

def increment_chunk_name(chunk_name, ix):
    """Recursive function that will increment chunk_name by one if possible

    Args:
        chunk_name (list): current chunk identifier as list
        ix (int): what position we are currently incrementing
    Returns:
        (str): Incremented chunk name
    """
    if chunk_name[ix] == 'Z':
        if ix > 0:
            chunk_name[ix] = 'A'
            return increment_chunk_name(chunk_name, ix - 1)
        else:
            smart_print('\033[93mWARNING:\033[0m Reached chunk limit. Please refactor code to accept' +
                        'more chunks.')
            sys.exit()
    else:
        chunk_name[ix] = chr(ord(chunk_name[ix]) + 1)
        return ''.join(chunk_name)


def write_dict(my_dict, out_dir, keys, chunk_name):
    """Append dictionary values to out_path

    Args:
        my_dict (dict): Dictionary containing values to be appended to out_path
        out_dir (str): Path for output file
        keys (list): Keys in specific order (just to ensure proper ordering)
        chunk_name (str): Which chunk is being written
    """
    out_path = '/'.join([out_dir, chunk_name])
    if len(my_dict[keys[0]]) == 0:
        return
    if os.path.exists(out_path):
        smart_print("\033[93mWOAH BUDDY\033[0m chunk number didn't increase")
    else:
        with open(out_path, 'w') as out_file:
            for key in keys:
                out_file.write('\t'.join(my_dict[key]) + '\n')


def consolidate_files(data_dir, out_path, features):
    """Consolidate the chunk files together

    Args:
        data_dir (str): Directory where chunks were stored
        out_path (str): Path to final file
        features (list): List of features to put there
    """
    smart_print("Consolidating chunks")
    for walk_tuple in os.walk(data_dir):
        file_names = walk_tuple[2]
        file_names = sorted(file_names)

        cur_pos = {file_name: 0 for file_name in file_names}

        out_f = open(out_path, 'w')
        for feature in features:
            cur_line = feature.decode() + '\t'
            for file_name in file_names:
                in_f = open('/'.join([data_dir, file_name]), 'r')
                in_f.seek(cur_pos[file_name])
                cur_line += in_f.readline().rstrip() + '\t'
                cur_pos[file_name] = in_f.tell()
                in_f.close()

            cur_line = cur_line.rstrip('\t') + '\n'
            out_f.write(cur_line)
        out_f.close()


def transpose_tsv(in_path, mpack_dir, out_path, data_dir,
                  gz_in, gz_out, n_data_points):
    """Transpose a normal TSV

    Args:
        in_path (str): Path to the input file
        mpack_dir (str): Path to MessagePack folder
        out_path (str): Path for the output file
        data_dir (str): Path for temporary directory (will be deleted at end of program)
        gz_in (bool): Is the input gzipped?
        gz_out (bool): Whether or not output should be gzipped
        n_data_points (int): Number of data points per chunk (determines chunk size)
    """
    samples = open_msgpack('/'.join([mpack_dir, 'samples.msgpack']), 'rb')
    features = open_msgpack('/'.join([mpack_dir, 'features.msgpack']), 'rb')

    nrows = len(samples) + 1
    ncols = len(features)

    chunk_size, n_chunks = calculate_chunks(nrows, ncols, n_data_points)

    smart_print(chunk_size)
    smart_print(nrows)

    rows_perc_chunk = (chunk_size / nrows) * 100
    cur_percentage = 0

    feature_dict = {}

    # If you want to allow more chunks, just add 'A's to the end (currently allows >11 million chunks)
    chunk_name = 'AAAAA'

    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

    os.mkdir(data_dir)

    try:

        with dynamic_open(in_path, mode='r', use_gzip_module=gz_in) as in_file:
            # We don't need the first line
            in_file.readline()

            for feature in features:
                feature_dict[feature] = []

            for ix, line in enumerate(in_file):
                if gz_in:
                    line = line.decode().strip().split('\t')
                else:
                    line = line.strip().split('\t')

                for feat_ix, feature in enumerate(features):
                    feature_dict[feature].append(line[feat_ix])

                if ix > 0 and ix % chunk_size == 0:
                    write_dict(feature_dict, data_dir, features, chunk_name)
                    feature_dict = reset_dict(feature_dict)
                    chunk_name = increment_chunk_name(list(chunk_name), len(chunk_name) - 1)
                    cur_percentage += rows_perc_chunk

                    ###############################################################
                    smart_print("{}% rows done".format(math.trunc(cur_percentage)))
                    ###############################################################

        #############################
        smart_print("100% rows done")
        #############################

        write_dict(feature_dict, data_dir, features, chunk_name)
        consolidate_files(data_dir, out_path, features)

        if gz_out:
            subprocess.check_call(['gzip', out_path])
    finally:
        shutil.rmtree(data_dir)
        smart_print("Done")

def merge_tsv(file_paths, mp_paths, prefs, out_path, chunk):
    """Put those files together

    :param file_paths: List of file paths to be merged
    :param mp_paths: List of paths to MessagePack directories in same order as file_paths
    :param prefs: List of prefixes for features, same length and order as file_paths
    :param out_path: String path to output file
    :param chunk: Int indicating rows per chunk
    :return:
    """
    cur_files = {in_f: dynamic_open(in_f, mode='r', use_gzip_module=in_f.endswith(".gz")) for in_f in file_paths}
    mp_samples = {mp_p: open_msgpack('/'.join([mp_p, "samples.msgpack"]), mode='rb') for mp_p in mp_paths}
    mp_features = {mp_p: open_msgpack('/'.join([mp_p, "features.msgpack"]), mode='rb') for mp_p in mp_paths}
    mp_maps = {mp_p: open_msgpack('/'.join([mp_p, "sample_data.msgpack"]), mode='rb') for mp_p in mp_paths}

    all_features = []

    all_lines = []

    if prefs is not None:
        for ix, mp_path in enumerate(mp_paths):

            mp_features[mp_path] = ['__'.join([prefs[ix], x.decode()]) for x in mp_features[mp_path]]
            all_features.extend(mp_features[mp_path])

        output = '\t'.join(["Sample", *[feat for feat in all_features if not feat.endswith('Sample')]]) + '\n'
    else:
        for mp_path in mp_paths:
            all_features.extend(mp_features[mp_path])
        output = '\t'.join(["Sample", *[feat.decode() for feat in all_features if feat != b'Sample']]) + '\n'

    out_file = open(out_path, mode='w')

    all_samples = sorted(list(set().union(*[v for k, v in mp_samples.items()])))

    for mp_p in mp_paths:
        mp_samples[mp_p] = set(mp_samples[mp_p])
    for ix, sample in enumerate(all_samples):
        output += sample.decode() + '\t'
        for i, mp_dir in enumerate(mp_paths):
            if sample in mp_samples[mp_dir]:
                cur_file = cur_files[file_paths[i]]
                start_ix = mp_maps[mp_dir][sample][0]
                cur_file.seek(start_ix)
                cur_line = cur_file.read(mp_maps[mp_dir][sample][1]) + '\t'
                output = ''.join([output, cur_line])
            else:
                output += '\t'.join(["NA"] * (len(mp_features[mp_dir]) - 1)) + '\t'
        output = output.rstrip('\t')
        output += '\n'
        all_lines.append(output)
        output = ''
        if ix > 0 and ix % chunk == 0:
            smart_print(ix)
            out_file.write(''.join(all_lines))
            all_lines = []

    out_file.write(''.join(all_lines))
    out_file.close()

    for cur_file in cur_files:
        cur_files[cur_file].close()
