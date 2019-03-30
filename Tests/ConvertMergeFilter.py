import os
import sys
pwd = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, pwd + "/..")
from DataSetBuilder import *
from DataSetParser import *

tmp_dir = sys.argv[1]

tsv_file_path_1 = "Tests/1.tsv"
tsv_file_path_2 = "Tests/2.tsv"
tsv_file_path_genes_1 = "Tests/Genes1.tsv"
tsv_file_path_genes_2 = "Tests/Genes2.tsv"
fwf_file_path_1 = "/{}/1.fwf".format(tmp_dir)
fwf_file_path_2 = "/{}/2.fwf".format(tmp_dir)
fwf_file_path_genes_1 = "/{}/Genes1.fwf".format(tmp_dir)
fwf_file_path_genes_2 = "/{}/Genes2.fwf".format(tmp_dir)
merged_file_path = "/{}/12.fwf".format(tmp_dir)
merged_genes_file_path = "/{}/Genes12.fwf".format(tmp_dir)
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
result = parser1.query([], [])
checkResult(result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'1', b'1.1', b'11.1', b'Low', b'High'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

# Standard mixed query
result = parser1.query([DiscreteFilter("TempA", ["Med"])], [NumericFilter("FloatA", ">", 0)], ["FloatB", "TempB"])
checkResult(result, [[b'Sample', b'FloatB', b'TempB'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

# Missing query
result = parser1.query([], [], ["FloatB", "TempB"])
checkResult(result, [[b'Sample', b'FloatB', b'TempB'], [b'1', b'11.1', b'High'], [b'2', b'22.2', b'Low'], [b'3', b'33.3', b'Med'], [b'4', b'44.4', b'Med']])

# No matching rows
result = parser1.query([], [NumericFilter("FloatA", "<", 0)], ["FloatB", "TempB"])
checkResult(result, [[b'Sample', b'FloatB', b'TempB']])

# Composite discrete filter
result = parser1.query([DiscreteFilter("TempB", ["Low", "Med"])], [])
checkResult(result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

# First and last columns and rows
result = parser1.query([DiscreteFilter("TempB", ["Low", "High"])], [NumericFilter("FloatA", ">", 0)], ["FloatA", "TempB"])
checkResult(result, [[b'Sample', b'FloatA', b'TempB'], [b'1', b'1.1', b'High'], [b'2', b'2.2', b'Low']])

# Filter based on int column
result = parser2.query([], [NumericFilter("IntB", ">", 35)], ["IntA", "ColorB"])
checkResult(result, [[b'Sample', b'IntA', b'ColorB'], [b'4', b'4', b'Brown'], [b'5', b'5', b'Orange']])

# Query by sample ID
result = parser1.query([], [NumericFilter("Sample", ">=", 3)])
checkResult(result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'3', b'3.3', b'33.3', b'Med', b'Med'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

# Equals
result = parser1.query([], [NumericFilter("Sample", "==", 3)])
checkResult(result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'3', b'3.3', b'33.3', b'Med', b'Med']])

# Not equals
result = parser1.query([], [NumericFilter("Sample", "!=", 3)])
checkResult(result, [[b'Sample', b'FloatA', b'FloatB', b'TempA', b'TempB'], [b'1', b'1.1', b'11.1', b'Low', b'High'], [b'2', b'2.2', b'22.2', b'High', b'Low'], [b'4', b'4.4', b'44.4', b'Med', b'Med']])

# Query merged data
result = parser12.query([DiscreteFilter("1__TempA", ["Med"]), DiscreteFilter("2__ColorB", ["Yellow"])], [NumericFilter("1__FloatA", ">", 0), NumericFilter("2__IntB", ">", 0)], ["1__FloatB", "1__TempB"], ["2"])
checkResult(result, [[b'Sample', b'1__FloatB', b'1__TempB', b'2__IntA', b'2__IntB', b'2__ColorA', b'2__ColorB'], [b'3', b'33.3', b'Med', b'3', b'33', b'Red', b'Yellow']])

# Missing values coming through properly in merged output
result = parser12.query([], [NumericFilter("Sample", "==", 1)])
checkResult(result, [[b'Sample', b'1__FloatA', b'1__FloatB', b'1__TempA', b'1__TempB', b'2__IntA', b'2__IntB', b'2__ColorA', b'2__ColorB'], [b'1', b'1.1', b'11.1', b'Low', b'High', b'', b'', b'', b'']])

checkResult(parser1.get_num_samples(), 4)
checkResult(parser2.get_num_samples(), 4)
checkResult(parser12.get_num_samples(), 5)
checkResult(parser1.get_num_variables(), 5)
checkResult(parser2.get_num_variables(), 5)
checkResult(parser12.get_num_variables(), 9)
checkResult(parser1.get_num_datapoints(), 20)
checkResult(parser2.get_num_datapoints(), 20)
checkResult(parser12.get_num_datapoints(), 45)

# Parse pathway information
pathway_gene_dict = build_pathway_gene_dict()
column_names_1 = parse_column_names(fwf_file_path_genes_1)
column_names_2 = parse_column_names(fwf_file_path_genes_2)
pathway_gene_indices_dict_1 = map_column_names_to_pathways(pathway_gene_dict, column_names_1)
pathway_gene_indices_dict_2 = map_column_names_to_pathways(pathway_gene_dict, column_names_2)
save_pathway_map_to_file(pathway_gene_indices_dict_1, fwf_file_path_genes_1)
save_pathway_map_to_file(pathway_gene_indices_dict_2, fwf_file_path_genes_2)

# Select columns by pathway
result = parser_genes1.query([], [], ["PHPT1"], [], ["Glycolysis / Gluconeogenesis [kegg]"])
checkResult(result, [[b'Sample', b'PGM1', b'PGM2', b'PFKP', b'PHPT1'], [b'1', b'5.2', b'3.8', b'1', b'2'], [b'2', b'6.4', b'9.2', b'1', b'2']])

result = parser_genes2.query([], [], [], [], ["Gene expression of MAFbx by FOXO ( Insulin receptor signaling (Mammal) ) [inoh]"])
checkResult(result, [[b'Sample', b'FOXO4', b'FOXO6'], [b'1', b'9', b'8'], [b'2', b'6', b'5']])

merge_fwf_files([fwf_file_path_genes_1, fwf_file_path_genes_2], merged_genes_file_path)
result = parser_genes12.query([], [], [], [], ["Metabolic pathways [kegg]"])
checkResult(result, [[b'Sample', b'Genes1__PGM1', b'Genes1__PGM2', b'Genes1__PFKP', b'Genes1__PMM1', b'Genes1__SORD', b'Genes2__AASS'], [b'1', b'5.2', b'3.8', b'1', b'3', b'4', b'7'], [b'2', b'6.4', b'9.2', b'1', b'3', b'4', b'4']])

# Not sure where we should be invoking this function, will differ depending on whether we do merge
#apply_aliases(tsv_file_path_1, fwf_file_path_1)

#build_metadata("Test", "Tests/description.md", "Tests/config.yaml", fwf_file_path_1)

#TODO: Store meta information for column types rather than (or in addition to) column types.
#  Description (besides num samples and num features)
#    get_id()
#  METADATA_PKL
#    get_variable() - set to None if len is greater than 100?
#    search_options()
#  GROUPS_JSON
#    get_groups() - set to None if len is greater than 100?
#    search_group()
#  get_num_samples_matching_filters() - Default to 0 for now.
#  get_num_variables_matching_filters() - Default to 0 for now.
#  get_num_datapoints_matching_filters() - Default to 0 for now.
#  Pathway data - look at Alyssa's email
#    Create a helper script in WishBuilder that can be called from each dataset script.
#    Add an example to Tests directory.
#    Probably just copy it over and rename it.
#    Remove BuildGmtFile...py
#    Modify query() so that it accepts pathways as well as groups.
#TODO: Remove commented lines from WishBuilder.py (after testing).
#TODO: Optimize performance by storing values we pull from files in class variables?
#TODO: Put data in pandas DataFrame and use ShapeShifter to convert to other formats (?).
#TODO: Build markdown files, etc. https://github.com/ercsuh/ercsuh.github.io
