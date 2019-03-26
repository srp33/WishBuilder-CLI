import os
import sys
pwd = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, pwd + "/..")
from FixedWidthHelper import *

tsv_file_path_1 = "Tests/1.tsv"
tsv_file_path_2 = "Tests/2.tsv"
fwf_file_path_1 = "/tmp/1.fwf"
fwf_file_path_2 = "/tmp/2.fwf"
merged_file_path = "/tmp/12.fwf"
out_file_path = "/tmp/12_output.tsv"

convert_tsv_to_fwf(tsv_file_path_1, fwf_file_path_1)
convert_tsv_to_fwf(tsv_file_path_2, fwf_file_path_2)

merge_fwf_files([fwf_file_path_1, fwf_file_path_2], merged_file_path)

query_fwf_file(merged_file_path, out_file_path)
#TODO: Store meta information for column types rather than (or in addition to) column types.
#TODO: Build pandas data frame
