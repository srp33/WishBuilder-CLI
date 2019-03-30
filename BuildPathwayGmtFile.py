import gzip
import os
import sys

in_data_file_path = sys.argv[1]
in_gmt_file_url = sys.argv[2]
out_gmt_file_path = sys.argv[3]

in_gmt_file_path = "/tmp/{}".format(os.path.basename(in_gmt_file_url))

os.system("wget -O {} {}".format(in_gmt_file_path, in_gmt_file_url))

datasource_dict = {}
gene_dict = {}

with gzip.open(in_gmt_file_path, 'rb') as in_gmt_file:
    for line in in_gmt_file:
        line_items = line.decode().rstrip("\n").split("\t")
        pathway_name = line_items[1].split(";")[0].replace("name: ", "").strip()
        data_source = line_items[1].split(";")[1].replace("datasource: ", "").strip()
        genes = line_items[2:]

        datasource_dict[pathway_name] = data_source
        gene_dict[pathway_name] = genes

os.remove(in_gmt_file_path)
