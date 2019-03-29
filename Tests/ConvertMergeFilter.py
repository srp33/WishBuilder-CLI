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

convert_tsv_to_fwf(tsv_file_path_1, fwf_file_path_1)
convert_tsv_to_fwf(tsv_file_path_2, fwf_file_path_2)

def checkResult(result, expected):
    if result != expected:
        print("Result:")
        print(result)
        print("Expected:")
        print(expected)
        sys.exit(1)
    else:
        print("Passed")

# Default values
result = query_fwf_file(fwf_file_path_1, [], [], [], [])
checkResult(result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'1', b'1.1', b'11.1', b'Low', b'High'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

# Standard mixed query
result = query_fwf_file(fwf_file_path_1, [DiscreteFilter("TempA", ["Med"])], [NumericFilter("FloatA", ">", 0)], ["FloatB", "TempB"], [])
checkResult(result, [[b'Sample', b'FloatB', b'TempB'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

# Missing query
result = query_fwf_file(fwf_file_path_1, [], [], ["FloatB", "TempB"], [])
checkResult(result, [[b'Sample', b'FloatB', b'TempB'], [b'1', b'11.1', b'High'], [b'2', b'22.2', b'Low'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

# No matching rows
result = query_fwf_file(fwf_file_path_1, [], [NumericFilter("FloatA", "<", 0)], ["FloatB", "TempB"], [])
checkResult(result, [[b'Sample', b'FloatB', b'TempB']])

# Composite discrete filter
result = query_fwf_file(fwf_file_path_1, [DiscreteFilter("TempB", ["Low", "Med"])], [], [], [])
checkResult(result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

# First and last columns and rows
result = query_fwf_file(fwf_file_path_1, [DiscreteFilter("TempB", ["Low", "High"])], [NumericFilter("FloatA", ">", 0)], ["FloatA", "TempB"], [])
checkResult(result, [[b'Sample', b'FloatA', b'TempB'], [b'1', b'1.1', b'High'], [b'2', b'2.2', b'Low']])

# Filter based on int column
result = query_fwf_file(fwf_file_path_2, [], [NumericFilter("IntB", ">", 35)], ["IntA", "ColorB"], [])
checkResult(result, [[b'Sample', b'ColorB', b'IntA'], [b'4', b'Brown', b'4'], [b'5', b'Orange', b'5']])

# Query by sample ID
result = query_fwf_file(fwf_file_path_1, [], [NumericFilter("Sample", ">=", 3)], [], [])
checkResult(result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

# Equals
result = query_fwf_file(fwf_file_path_1, [], [NumericFilter("Sample", "==", 3)], [], [])
checkResult(result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'3', b'3.3', b'33.3', b'Med', b'Med']])

# Not equals
result = query_fwf_file(fwf_file_path_1, [], [NumericFilter("Sample", "!=", 3)], [], [])
checkResult(result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'1', b'1.1', b'11.1', b'Low', b'High'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

merge_fwf_files([fwf_file_path_1, fwf_file_path_2], merged_file_path)

# Query merged data
result = query_fwf_file(merged_file_path, [DiscreteFilter("1__TempA", ["Med"]), DiscreteFilter("2__ColorB", ["Yellow"])], [NumericFilter("1__FloatA", ">", 0), NumericFilter("2__IntB", ">", 0)], ["1__FloatB", "1__TempB"], ["2"])
checkResult(result, [[b'Sample', b'1__FloatB', b'1__TempB', b'2__ColorA', b'2__ColorB', b'2__IntA', b'2__IntB'], [b'3', b'33.3', b'Med', b'Red', b'Yellow', b'3', b'33']])

# Missing values coming through properly in merged output
result = query_fwf_file(merged_file_path, [], [NumericFilter("Sample", "==", 1)], [], [])
checkResult(result, [[b'Sample', b'1__FloatA', b'1__FloatB', b'1__TempA', b'1__TempB', b'2__ColorA', b'2__ColorB', b'2__IntA', b'2__IntB'], [b'1', b'1.1', b'11.1', b'Low', b'High', b'', b'', b'', b'']])

#TODO: Store meta information for column types rather than (or in addition to) column types.
#  num_samples()
#  num_features()
#  get_num_data_points()
#  Description (besides num samples and num features)
#  METADATA_PKL
#    get_variable() - set to None if len is greater than 100?
#    search_options()
#  GROUPS_JSON
#    get_groups() - set to None if len is greater than 100?
#    search_group()
#  get_num_samples_matching_filters() - Default to 0 for now.
#  query_samples() - do we need this still?
#  query()
#  get_file_collection()
#TODO: Put data in pandas DataFrame and use ShapeShifter to convert to other formats (?).
