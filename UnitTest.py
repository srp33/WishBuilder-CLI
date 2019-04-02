import os
import sys
pwd = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, pwd + "/..")
from DataSetBuilder import *
from DataSetParser import *

tmp_dir = sys.argv[1]

tsv_file_path_1 = "TestData/Test1/data.tsv"
tsv_file_path_2 = "TestData/Test2/data.tsv"
tsv_file_path_genes_1 = "TestData/Genes1/data.tsv"
tsv_file_path_genes_2 = "TestData/Genes2/data.tsv"
fwf_file_path_1 = "{}/1.fwf".format(tmp_dir)
fwf_file_path_2 = "{}/2.fwf".format(tmp_dir)
fwf_file_path_genes_1 = "{}/Genes1.fwf".format(tmp_dir)
fwf_file_path_genes_2 = "{}/Genes2.fwf".format(tmp_dir)
merged_file_path = "{}/12.fwf".format(tmp_dir)
merged_genes_file_path = "{}/Genes12.fwf".format(tmp_dir)
parser1 = DataSetParser(fwf_file_path_1)
parser2 = DataSetParser(fwf_file_path_2)
parser12 = DataSetParser(merged_file_path)
parser_genes1 = DataSetParser(fwf_file_path_genes_1)
parser_genes2 = DataSetParser(fwf_file_path_genes_2)
parser_genes12 = DataSetParser(merged_genes_file_path)

convert_tsv_to_fwf("Test1", tsv_file_path_1, fwf_file_path_1)
convert_tsv_to_fwf("Test2", tsv_file_path_2, fwf_file_path_2)
convert_tsv_to_fwf("Genes1", tsv_file_path_genes_1, fwf_file_path_genes_1)
convert_tsv_to_fwf("Genes2", tsv_file_path_genes_2, fwf_file_path_genes_2)

merge_fwf_files([fwf_file_path_1, fwf_file_path_2], merged_file_path)

def checkResult(description, result, expected):
    if result != expected:
        print("{}".format(description))
        print("Result:")
        print(result)
        print("Expected:")
        print(expected)
        sys.exit(1)
    else:
        print("Passed")

checkResult("ID", parser1.id, "Test1")
checkResult("Title 1", parser1.title, "This is the title1")
checkResult("ID", parser2.id, "Test2")
checkResult("Title 2", parser2.title, "This is the title2")
checkResult("Num samples 1", parser1.num_samples, 4)
checkResult("Num samples 2", parser2.num_samples, 4)
checkResult("Num samples 3", parser12.num_samples, 5)
checkResult("Num features 1", parser1.num_features, 5)
checkResult("Num features 2", parser2.num_features, 5)
checkResult("Num features 3", parser12.num_features, 9)
checkResult("Num datapoints 1", parser1.total_datapoints, 20)
checkResult("Num datapoints 2", parser2.total_datapoints, 20)
checkResult("Num datapoints 3", parser12.total_datapoints, 45)

result = parser1.query([], [])
checkResult("Default values", result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'1', b'1.1', b'11.1', b'Low', b'High'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

result = parser1.query([DiscreteFilter("TempA", ["Med"])], [NumericFilter("FloatA", ">", 0)], ["FloatB", "TempB"])
checkResult("Standard mixed query", result, [[b'Sample', b'FloatB', b'TempB'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

result = parser1.query([], [], ["FloatB", "TempB"])
checkResult("Missing query", result, [[b'Sample', b'FloatB', b'TempB'], [b'1', b'11.1', b'High'], [b'2', b'22.2', b'Low'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

result = parser1.query([], [NumericFilter("FloatA", "<", 0)], ["FloatB", "TempB"])
checkResult("No matching rows", result, [[b'Sample', b'FloatB', b'TempB']])

result = parser1.query([DiscreteFilter("TempB", ["Low", "Med"])], [])
checkResult("Composite discrete filter", result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

result = parser1.query([DiscreteFilter("TempB", ["Low", "High"])], [NumericFilter("FloatA", ">", 0)], ["FloatA", "TempB"])
checkResult("First and last columns and rows", result, [[b'Sample', b'FloatA', b'TempB'], [b'1', b'1.1', b'High'], [b'2', b'2.2', b'Low']])

result = parser2.query([], [NumericFilter("IntB", ">", 35)], ["IntA", "ColorB"])
checkResult("Filter based on int column", result, [[b'Sample', b'IntA', b'ColorB'], [b'4', b'4', b'Brown'], [b'5', b'5', b'Orange']])

result = parser1.query([], [NumericFilter("Sample", ">=", 3)])
checkResult("Query by sample ID", result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

result = parser1.query([], [NumericFilter("Sample", "==", 3)])
checkResult("Equals", result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'3', b'3.3', b'33.3', b'Med', b'Med']])

result = parser1.query([], [NumericFilter("Sample", "!=", 3)])
checkResult("Not equals", result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'1', b'1.1', b'11.1', b'Low', b'High'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

result = parser12.query([DiscreteFilter("1__TempA", ["Med"]), DiscreteFilter("2__ColorB", ["Yellow"])], [NumericFilter("1__FloatA", ">", 0), NumericFilter("2__IntB", ">", 0)], ["1__FloatB", "1__TempB"], ["2"])
checkResult("Query merged data + group", result, [[b'Sample', b'1__FloatB', b'1__TempB', b'2__IntA', b'2__IntB', b'2__ColorA', b'2__ColorB'], [b'3', b'33.3', b'Med', b'3', b'33', b'Red', b'Yellow']])

result = parser12.query([], [NumericFilter("Sample", "==", 1)])
checkResult("Missing values coming through properly in merged output", result, [[b'Sample', b'1__FloatA', b'1__FloatB', b'1__TempA', b'1__TempB', b'2__IntA', b'2__IntB', b'2__ColorA', b'2__ColorB'], [b'1', b'1.1', b'11.1', b'Low', b'High', b'', b'', b'', b'']])

result = parser_genes1.query([], [], ["PHPT1"], [], ["Glycolysis / Gluconeogenesis [kegg]"])
checkResult("Select columns by pathway 1", result, [[b'Sample', b'PGM1', b'PGM2', b'PFKP', b'PHPT1'], [b'1', b'5.2', b'3.8', b'1', b'2'], [b'2', b'6.4', b'9.2', b'1', b'2']])

result = parser_genes2.query([], [], [], [], ["Gene expression of MAFbx by FOXO ( Insulin receptor signaling (Mammal) ) [inoh]"])
checkResult("Select columns by pathway 2", result, [[b'Sample', b'FOXO4', b'FOXO6'], [b'1', b'9', b'8'], [b'2', b'6', b'5']])

merge_fwf_files([fwf_file_path_genes_1, fwf_file_path_genes_2], merged_genes_file_path)
result = parser_genes12.query([], [], [], [], ["Metabolic pathways [kegg]"])
checkResult("Merge genes", result, [[b'Sample', b'Genes1__PGM1', b'Genes1__PGM2', b'Genes1__PFKP', b'Genes1__PMM1', b'Genes1__SORD', b'Genes2__AASS'], [b'1', b'5.2', b'3.8', b'1', b'3', b'4', b'7'], [b'2', b'6.4', b'9.2', b'1', b'3', b'4', b'4']])

alias_dict = build_alias_dict(tsv_file_path_1)
apply_aliases(alias_dict, fwf_file_path_1)
result = parser1.query([DiscreteFilter("TempA_alias", ["Med"])], [NumericFilter("FloatA_alias", ">", 0)], ["FloatB", "TempB"])
checkResult("Apply aliases to individual dataset", result, [[b'Sample', b'FloatB', b'TempB'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

#TODO: Should support parsing multiple alias dicts.
alias_dict_genes = build_alias_dict(tsv_file_path_genes_1)
merge_fwf_files([fwf_file_path_genes_1, fwf_file_path_genes_2], merged_genes_file_path, alias_dict_genes)
result = parser_genes12.query([], [], [], [], ["Metabolic pathways [kegg]"])
checkResult("Apply aliases to merged dataset", result, [[b'Sample', b'Genes1__PGM1_alias', b'Genes1__PGM2_alias', b'Genes1__PFKP_alias', b'Genes1__PMM1_alias', b'Genes1__SORD', b'Genes2__AASS'], [b'1', b'5.2', b'3.8', b'1', b'3', b'4', b'7'], [b'2', b'6.4', b'9.2', b'1', b'3', b'4', b'4']])

#TODO:
#  Description (besides num samples and num features)
#    get_id()
#  METADATA_PKL
#    get_variable() - set to None if len is greater than 100?
#    search_options()
#  GROUPS_JSON
#    get_groups() - set to None if len is greater than 100?
#    search_group()
#  get_num_samples_matching_filters() - Default to 0 for now. - But let's implement it.
#  get_num_variables_matching_filters() - Default to 0 for now. - But let's implement it.
#  get_num_datapoints_matching_filters() - Default to 0 for now. - But let's implement it.
#TODO: Remove commented lines from WishBuilder.py (after testing).
#TODO: Optimize performance by storing values we pull from files in class variables?
#TODO: Put data in pandas DataFrame and use ShapeShifter to convert to other formats (?).
#TODO: Build markdown files, etc. https://github.com/ercsuh/ercsuh.github.io
