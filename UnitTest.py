import os
import sys
pwd = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, pwd + "/..")
from DataSetBuilder import *
from DataSetParser import *

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

def checkResultFile(description, file_path, expected):
    file_contents = readFileIntoLists(query_file_path)
    checkResult(description, file_contents, expected)

def readFileIntoLists(file_path):
    with open(file_path, "rb") as the_file:
        out_items = []

        for line in the_file:
            out_items.append(line.rstrip(b"\n").split(b"\t"))

        return out_items

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
query_file_path = "{}/output.tsv".format(tmp_dir)
parser1 = DataSetParser(fwf_file_path_1)
parser2 = DataSetParser(fwf_file_path_2)
parser12 = DataSetParser(merged_file_path)
parser_genes1 = DataSetParser(fwf_file_path_genes_1)
parser_genes2 = DataSetParser(fwf_file_path_genes_2)
parser_genes12 = DataSetParser(merged_genes_file_path)

convert_tsv_to_fwf(tsv_file_path_1, fwf_file_path_1)
convert_tsv_to_fwf(tsv_file_path_2, fwf_file_path_2)
convert_tsv_to_fwf(tsv_file_path_genes_1, fwf_file_path_genes_1)
convert_tsv_to_fwf(tsv_file_path_genes_2, fwf_file_path_genes_2)

merge_fwf_files([fwf_file_path_1, fwf_file_path_2], merged_file_path)
merge_fwf_files([fwf_file_path_genes_1, fwf_file_path_genes_2], merged_genes_file_path)

build_metadata("TestData/Test1", fwf_file_path_1)
build_metadata("TestData/Test2", fwf_file_path_2)
build_metadata("TestData/Genes1", fwf_file_path_genes_1)
build_metadata("TestData/Genes2", fwf_file_path_genes_2)
build_metadata("TestData/Test1", merged_file_path)
build_metadata("TestData/Genes1", merged_genes_file_path)

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

parser1.query([], [], [], [], [], query_file_path)
checkResultFile("Default values", query_file_path, [[b'Sample', b'FloatA_alias', b'FloatB', b'TempA_alias', b'TempB'], [b'1', b'1.1', b'11.1', b'Low', b'High'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

#parser1.query([DiscreteFilter("TempA_alias", ["Med"])], [NumericFilter("FloatA_alias", ">", 0)], ["FloatB", "TempB"], [], [], query_file_path)
parser1.query([DiscreteFilter(3, ["Med"])], [NumericFilter(1, ">", 0)], [2, 4], [], [], query_file_path)
checkResultFile("Standard mixed query", query_file_path, [[b'Sample', b'FloatB', b'TempB'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

#parser1.query([], [], ["FloatB", "TempB"], [], [], query_file_path)
parser1.query([], [], [2, 4], [], [], query_file_path)
checkResultFile("Missing query", query_file_path, [[b'Sample', b'FloatB', b'TempB'], [b'1', b'11.1', b'High'], [b'2', b'22.2', b'Low'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

#parser1.query([], [NumericFilter("FloatA_alias", "<", 0)], ["FloatB", "TempB"], [], [], query_file_path)
parser1.query([], [NumericFilter(1, "<", 0)], [2, 4], [], [], query_file_path)
checkResultFile("No matching rows", query_file_path, [[b'Sample', b'FloatB', b'TempB']])

parser1.query([DiscreteFilter(4, ["Low", "Med"])], [], [], [], [], query_file_path)
checkResultFile("Composite discrete filter", query_file_path, [[b'Sample', b'FloatA_alias', b'FloatB', b'TempA_alias', b'TempB'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

#parser1.query([DiscreteFilter("TempB", ["Low", "High"])], [NumericFilter("FloatA_alias", ">", 0)], ["FloatA_alias", "TempB"], [], [], query_file_path)
parser1.query([DiscreteFilter(4, ["Low", "High"])], [NumericFilter(1, ">", 0)], [1, 4], [], [], query_file_path)
checkResultFile("First and last columns and rows", query_file_path, [[b'Sample', b'FloatA_alias', b'TempB'], [b'1', b'1.1', b'High'], [b'2', b'2.2', b'Low']])

#parser2.query([], [NumericFilter("IntB", ">", 35)], ["IntA", "ColorB"], [], [], query_file_path)
parser2.query([], [NumericFilter(2, ">", 35)], [1, 4], [], [], query_file_path)
checkResultFile("Filter based on int column", query_file_path, [[b'Sample', b'IntA', b'ColorB'], [b'4', b'4', b'Brown'], [b'5', b'5', b'Orange']])

parser1.query([], [NumericFilter(0, ">=", 3)], [], [], [], query_file_path)
checkResultFile("Query by sample ID", query_file_path, [[b'Sample', b'FloatA_alias', b'FloatB', b'TempA_alias', b'TempB'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

parser1.query([], [NumericFilter(0, "==", 3)], [], [], [], query_file_path)
checkResultFile("Equals", query_file_path, [[b'Sample', b'FloatA_alias', b'FloatB', b'TempA_alias', b'TempB'], [b'3', b'3.3', b'33.3', b'Med', b'Med']])

parser1.query([], [NumericFilter(0, "!=", 3)], [], [], [], query_file_path)
checkResultFile("Not equals", query_file_path, [[b'Sample', b'FloatA_alias', b'FloatB', b'TempA_alias', b'TempB'], [b'1', b'1.1', b'11.1', b'Low', b'High'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

#parser12.query([DiscreteFilter("1__TempA_alias", ["Med"]), DiscreteFilter("2__ColorB", ["Yellow"])], [NumericFilter("1__FloatA_alias", ">", 0), NumericFilter("2__IntB", ">", 0)], ["1__FloatB", "1__TempB"], ["2"], [], query_file_path)
parser12.query([DiscreteFilter(3, ["Med"]), DiscreteFilter(8, ["Yellow"])], [NumericFilter(1, ">", 0), NumericFilter(2, ">", 0)], [2, 4], ["2"], [], query_file_path)
checkResultFile("Query merged data + group", query_file_path, [[b'Sample', b'1__FloatB', b'1__TempB', b'2__IntA', b'2__IntB', b'2__ColorA', b'2__ColorB'], [b'3', b'33.3', b'Med', b'3', b'33', b'Red', b'Yellow']])

parser12.query([], [NumericFilter(0, "==", 1)], [], [], [], query_file_path)
checkResultFile("Missing values coming through properly in merged output", query_file_path, [[b'Sample', b'1__FloatA_alias', b'1__FloatB', b'1__TempA_alias', b'1__TempB', b'2__IntA', b'2__IntB', b'2__ColorA', b'2__ColorB'], [b'1', b'1.1', b'11.1', b'Low', b'High', b'', b'', b'', b'']])

#parser_genes1.query([], [], ["PHPT1_alias"], [], ["Glycolysis / Gluconeogenesis [kegg]"], query_file_path)
parser_genes1.query([], [], [4], [], ["Glycolysis / Gluconeogenesis [kegg]"], query_file_path)
checkResultFile("Select columns by pathway 1", query_file_path, [[b'Sample', b'PGM1_alias', b'PGM2_alias', b'PFKP_alias', b'PHPT1_alias'], [b'1', b'5.2', b'3.8', b'1', b'2'], [b'2', b'6.4', b'9.2', b'1', b'2']])

parser_genes2.query([], [], [], [], ["Gene expression of MAFbx by FOXO ( Insulin receptor signaling (Mammal) ) [inoh]"], query_file_path)
checkResultFile("Select columns by pathway 2", query_file_path, [[b'Sample', b'FOXO4', b'FOXO6'], [b'1', b'9', b'8'], [b'2', b'6', b'5']])

parser_genes12.query([], [], [], [], ["Metabolic pathways [kegg]"], query_file_path)
checkResultFile("Merge genes", query_file_path, [[b'Sample', b'Genes1__PGM1_alias', b'Genes1__PGM2_alias', b'Genes1__PFKP_alias', b'Genes1__PMM1_alias', b'Genes1__SORD', b'Genes2__AASS'], [b'1', b'5.2', b'3.8', b'1', b'3', b'4', b'7'], [b'2', b'6.4', b'9.2', b'1', b'3', b'4', b'4']])

#parser1.query([DiscreteFilter("TempA_alias", ["Med"])], [NumericFilter("FloatA_alias", ">", 0)], ["FloatB", "TempB"], [], [], query_file_path)
parser1.query([DiscreteFilter(3, ["Med"])], [NumericFilter(1, ">", 0)], [2, 4], [], [], query_file_path)
checkResultFile("Apply aliases to individual dataset", query_file_path, [[b'Sample', b'FloatB', b'TempB'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

parser_genes12.query([], [], [], [], ["Metabolic pathways [kegg]"], query_file_path)
checkResultFile("Apply aliases to merged dataset", query_file_path, [[b'Sample', b'Genes1__PGM1_alias', b'Genes1__PGM2_alias', b'Genes1__PFKP_alias', b'Genes1__PMM1_alias', b'Genes1__SORD', b'Genes2__AASS'], [b'1', b'5.2', b'3.8', b'1', b'3', b'4', b'7'], [b'2', b'6.4', b'9.2', b'1', b'3', b'4', b'4']])

checkResult("1 group", len(parser1.get_groups()), 1)
checkResult("1 group - search - no match", len(parser1.search_group("data", "blah")), 0)
checkResult("1 group - search - match", len(parser1.search_group("data", "Float")), 2)
checkResult("2 groups", len(parser12.get_groups()), 2)
checkResult("2 groups - group 1 values", parser12.search_group("1"), [(1, 'FloatA_alias'), (2, 'FloatB'), (3, 'TempA_alias'), (4, 'TempB')])
checkResult("2 groups - group 1 values - search", parser12.search_group("1", "F"), [(1, 'FloatA_alias'), (2, 'FloatB')])
checkResult("2 groups - group 1 values - search - max", parser12.search_group("1", "F", 2), [(1, 'FloatA_alias'), (2, 'FloatB')])
checkResult("2 groups - group 1 values - search - beyond max", parser12.search_group("1", "F", 1), [(1, 'FloatA_alias')])
checkResult("2 groups - group 2 values - search", parser12.search_group("2", "olor"), [(7, 'ColorA'), (8, 'ColorB')])

checkResult("No pathways 1", len(parser1.get_pathways()), 0)
checkResult("No pathways 12", len(parser12.get_pathways()), 0)
checkResult("Pathways genes1", len(parser_genes1.get_pathways()), 36)
checkResult("Pathways genes12", len(parser_genes12.get_pathways()), 63)
checkResult("Pathways genes12 - element", parser_genes12.get_pathways()[0], ('Glycolysis / Gluconeogenesis [kegg]', 3))

checkResult("Get sample column meta", parser1.get_variable_meta(0), (4, ['1', '2', '3', '4']))
checkResult("Get num column meta", parser1.get_variable_meta(1), (1.1, 4.4))
checkResult("Get discrete column meta", parser1.get_variable_meta(3), (3, ['High', 'Low', 'Med']))
checkResult("Get discrete column meta - beyond max", parser1.get_variable_meta(3, max_discrete_options=2), (3, None))

checkResult("Check sample column options", parser1.search_discrete_variable_options(0, search_str=None, max_discrete_options=100), ['1', '2', '3', '4'])
checkResult("Check sample column options - max", parser1.search_discrete_variable_options(0, search_str=None, max_discrete_options=4), ['1', '2', '3', '4'])
checkResult("Check sample column options - beyond max", parser1.search_discrete_variable_options(0, search_str=None, max_discrete_options=2), ['1', '2'])
checkResult("Check sample column options - search", parser1.search_discrete_variable_options(3, search_str="d", max_discrete_options=2), ['Med'])

parser1.clean_up(max_age_seconds=0)
parser1.save_sample_indices_matching_filters([], [])
checkResult("Clean up", parser1.clean_up(max_age_seconds=0), 1)

print("Passed all tests!!")

#TODO:
#  Provide a way to stream a file?
#TODO: Process GSE10320 using the updated code.
#TODO: Remove commented lines from WishBuilder.py (after testing).
#TODO: Put data in pandas DataFrame and use ShapeShifter (?) to convert to other formats.
#TODO: Build markdown files, etc. https://github.com/ercsuh/ercsuh.github.io
