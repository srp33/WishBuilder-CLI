import gzip, os, sys
from Constants import *
import pandas as pd
from PullRequest import PullRequest
from Shared import *

def one_feature(filePath):
    print("Reading data for the one_feature function from {}".format(filePath))
    df = pd.read_csv(filePath, sep='\t', low_memory=False)

    if len(df.columns.values) > 2:
        return True
    else:
        return False

# Are all test files in correct format?
def check_test_files(test_file_list):
    min_samples = MIN_SAMPLES
    min_test_cases = MIN_TEST_CASES
    report = ''
    passed = True

    for file in test_file_list:
        min_features = MIN_FEATURES
        data_file = '{}.gz'.format(file.lstrip('test_'))
        if one_feature(data_file):
            min_features = 1

        row_count = 0
        samples = {}

        report += "#### Running \"{0}\"\n\n".format(file)

        with open(file, 'r') as test_file:
            headers = test_file.readline().rstrip('\n').split('\t')
            # Make sure there are three columns named Sample, Variable, Value
            passed, temp_report = check_test_columns(headers, file)
            if passed:
                report += "{check_mark}\t\"{0}\" has three columns with the correct headers\n\n"\
                    .format(file, check_mark=CHECK_MARK)
            else:
                report += temp_report

            for line in test_file:
                row_count += 1
                data = line.rstrip('\n').split('\t')
                if len(data) is not 3 and len(data) is not 0:  # Make sure each row has exactly three columns
                    report += "{red_x}\tRow {0} of \"{1}\" should contain exactly three columns\n\n"\
                        .format(row_count, file, red_x=RED_X)
                    passed = False
                elif len(data) != 0:  # Add data to a map
                    if data[0] not in samples.keys():
                        samples[data[0]] = [data[1] + data[2]]
                    else:
                        if data[1] + data[2] not in samples[data[0]]:
                            samples[data[0]].append(data[1]+data[2])

        if len(samples.keys()) < min_samples:  # Make sure there are enough unique sample IDs to test
            report += "{red_x}\t\"{0}\" does not contain enough unique samples to test (min: {1})\n\n"\
                .format(file, min_samples, red_x=RED_X)
            passed = False
        else:
            report += "{check_mark}\t\"{0}\" contains enough unique samples to test\n\n"\
                .format(file, check_mark=CHECK_MARK)

        for sample in samples:  # Make sure each sample has enough features to test
            if len(samples[sample]) < min_features:
                report += "{red_x}\tSample \"{0}\" does not have enough features to test (min: {1})\n\n"\
                    .format(sample, min_features, red_x=RED_X)
                passed = False

        if passed:
            report += "{check_mark}\t\"{0}\" has enough features to test (min: {1})\n\n"\
                .format(file, min_features, check_mark=CHECK_MARK)

        if row_count == 0:  # Check if file is empty
            report += "{red_x}\t\"{0}\" is empty.\n\n".format(file, red_x=RED_X)
            passed = False
        elif row_count < min_test_cases:  # Check if there are enough test cases
            report += "{red_x}\t\"{0}\" does not contain enough test cases ({1}; min: {2})\n\n"\
                .format(file, row_count, min_test_cases, red_x=RED_X)
            passed = False
        else:
            report += "{check_mark}\t\"{0}\" contains enough test cases ({1}; min: {2})\n\n"\
                .format(file, row_count, min_test_cases, check_mark=CHECK_MARK)

    if passed:
        report += "#### Results: PASS\n---\n"
    else:
        report += "#### Results: FAIL\n---\n"

    return report, passed

# Check if the column headers of the test file are "Sample", "Variable", and "Value"
def check_test_columns(col_headers, file):
    passed = True
    report = ""

    if len(col_headers) != 3:  # Make sure there are exactly three columns
        report += "{red_x}\t\"{0}\" does not contain three columns\n\n".format(file, red_x=RED_X)
        passed = False
    else:  # Check the names of each column
        if col_headers[0] != "Sample" and col_headers[0] != "SampleID":
            report += "{red_x}\tFirst column of \"{0}\" must be titled \"Sample\"\n\n'".format(file, red_x=RED_X)
            passed = False
        if col_headers[1] != "Variable":
            report += "{red_x}\tSecond column of \"{0}\" must be titled \"Variable\"\n\n".format(file, red_x=RED_X)
            passed = False
        if col_headers[2] != "Value":
            report += "{red_x}\tThird column of \"{0}\" must be titled \"Value\"\n\n')".format(file, red_x=RED_X)
            passed = False

    return passed, report

def compare_files(data_file_list, test_file_list):
    passed_all = True
    report = "### Comparing Files:\n\n"
    all_samples = {}

    for data_file_path in data_file_list:
        passed = True

        test_file_path = 'test_' + data_file_path.rstrip('.gz')
        test_dict = {}

        with open(test_file_path, 'r') as test_file:
            column_headers = test_file.readline().decode().rstrip('\n').split('\t')

            for line in test_file:
                test_data = line.decode().rstrip('\n').split('\t')
                sample = test_data[0]
                variable = test_data[1]
                value = test_data[2]

                if sample not in test_dict:
                    test_dict[sample] = {}

                test_dict[sample][variable] = value

        with gzip.open(file, 'r') as data_file:
            # ----------------------------------------------------------------------------------------------------------
            for test in test_file_list:  # Get matching test file
                if file.rstrip('.gz') == test.lstrip('test_'):
                    test_file_name = test


            # ----------------------------------------------------------------------------------------------------------

            report += "#### Comparing \"{0}\" and \"{1}\"\n\n".format(file, test_file_name)

            report += create_html_table(NUM_SAMPLE_COLUMNS, NUM_SAMPLE_ROWS, file)

            data_headers = data_file.readline().decode().rstrip('\n').split('\t')

            for variable in data_headers:  # Make sure column headers are unique in data file
                if variable not in column_headers:
                    column_headers.append(variable)
                else:
                    passed = False
                    report += "{red_x}\t{0} is in \"{1}\" column headers more than once\n\n"\
                        .format(variable, file, red_x=RED_X)

            if data_headers[0] != "Sample":  # Make sure first column header is named "Sample"
                report += "{red_x}\tFirst column of \"{0}\" must be titled \"Sample\"\n\n".format(file, red_x=RED_X)
            else:
                report += "{check_mark}\tFirst column of \"{0}\" is titled \"Sample\"\n\n"\
                    .format(file, check_mark=CHECK_MARK)

            # PARSING THROUGH DATA FILE
            for line in data_file:
                data_row_count += 1
                data = line.decode().rstrip('\n').split('\t')
                sample = data[0]
                data_samples.add(sample)  # Add sample IDs in data file to the set

                if sample in test_dict.keys():
                    for info in test_dict[sample]:
                        row = info[3]  # row number from test file
                        tested_rows.append(row)  # Keep track of rows checked

                        if info[1] in data_headers:  # Check if variable in test file is in data file
                            variable_index = data_headers.index(info[1])
                            if info[2] == data[variable_index]:  # Check if variable's value in test file matches value in data file
                                report += "{check_mark}\tRow {0}: Success\n\n".format(row, check_mark=CHECK_MARK)
                            else:
                                passed = False
                                report += "{red_x}\tRow {0}: Fail - Value in test file ({1}) does not match value in data file ({2}) for {3} and {4}.\n\n".format(row, info[2], data[variable_index], info[0], info[1], red_x=RED_X)
                        else:  # Make sure
                            report += "{red_x}\tRow {0} - Variable \"{1}\" is not found in \"{2}\" column headers\n\n"\
                                .format(row, info[1], file, red_x=RED_X)
                            passed = False

            if len(tested_rows) < test_row_count:
                passed = False
                for i in range(test_row_count):
                    if i + 1 not in tested_rows:
                        report += "{red_x}\tRow {0} - Sample \"{1}\" from {2} is not found in \"{3}\"\n\n"\
                            .format(i + 1, test_samples[i], test_file_name, file, red_x=RED_X)

            if not passed:
                passed_all = False
        all_samples[file] = data_samples

    report += "### Comparing Samples\n\n"
    samples = False
    pass_sample_comparison = True
    num_samples = 0
    for file in all_samples.keys():
        if not samples:
            samples = all_samples[file]
            num_samples = len(samples)
        else:
            if all_samples[file] != samples:
                report += "{}\tSamples in data files are not equal\n\n".format(RED_X)
                pass_sample_comparison = False
                passed_all = False

    if pass_sample_comparison:
        report += "{}\tSamples are the same in all data files\n\n".format(CHECK_MARK)

    if passed_all:
        report += "#### Results: PASS\n---\n"
    else:
        report += "#### Results: FAIL\n---\n"

    return report, passed_all, num_samples

# Check if there is a test file for every data file
def check_test_for_every_data(pr: PullRequest, file_list):
    report = "### Testing Test Files:\n\n"
    data_files = []
    test_files = []
    bad_data_files = []
    bad_test_files = []
    passed = True

    for f in file_list:
        if f.endswith(".tsv.gz"):
            data_files.append(f)
            bad_data_files.append(f)
        elif f.startswith("test_"):
            test_files.append(f)
            bad_test_files.append(f)

    for data in data_files:
        for test in test_files:
            if data.rstrip('.gz') == test.lstrip('test_'):
                bad_data_files.remove(data)
                bad_test_files.remove(test)

    if (len(bad_data_files) + len(bad_test_files)) > 0:
        passed = False

    if not passed:
        for f in bad_data_files:
            report += "{}\tData file {} is missing required test file \"test_{}.tsv\"\n\n".format(RED_X, f, f.rstrip('.tsv.gz'))
        for f in bad_test_files:
            report += "{}\tTest file {} is missing required data file \"{}.gz\"\n\n".format(RED_X, f, f.lstrip('test_'))

        report += "#### Results: FAIL\n\n\n"
        pr.report.key_test_report = report
        pr.report.pass_key_test = False
    else:
        r, passed = check_test_files(test_files)
        report += r
        pr.report.key_test_report = report
        pr.report.pass_key_test = passed

        if passed:
            report, passed, num_samples = compare_files(data_files, test_files)
            pr.num_samples = num_samples
            pr.report.data_tests_report = report
            pr.report.pass_data_tests = passed

    return passed

#def convert(file):
#    if 'metadata' in file:
#        df = pd.read_csv(file, sep='\t')
#        if len(df.columns.values) == 3:
#            if 'Variable' in df.columns.values and 'Value' in df.columns.values:
#                metadata = {}
#                with gzip.open(file) as fp:
#                    fp.readline()
#                    for line in fp:
#                        data = line.decode().rstrip('\n').split('\t')
#                        metadata.setdefault(data[0], {})[data[1]] = data[2]
#                df = pd.DataFrame(metadata).T
#                df.to_csv(file, compression='gzip', sep='\t', index_label='Sample')

#def create_md_table(columns, rows, file_path):
#    table = ''
#    with gzip.open(file_path, 'r') as inFile:
#        for i in range(rows):
#            line = inFile.readline().decode().rstrip('\n').split('\t')
#            if len(line) < columns:
#                columns = len(line)
#            if i == 0:
#                table = '\n### First ' + \
#                        str(columns) + ' columns and ' + str(rows) + \
#                        ' rows of ' + file_path + ':\n\n'
#            if i == 1:
#                for j in range(columns):
#                    table += '|\t---\t'
#                table += '|\n'
#            table = table + '|'
#            for j in range(columns):
#                table = table + '\t' + line[j] + '\t|'
#            table = table + '\n'
#    table += '\n'
#    return table

def create_html_table(columns, rows, file_path):
    table = '\n### First ' + str(columns) + ' columns and ' + str(rows) + ' rows of ' + file_path + ':\n\n'
    table += '<table style="width:100%; border: 1px solid black;">\n'
    with gzip.open(file_path, 'r') as inFile:
        for i in range(rows):
            table += "\t<tr align='left'>\n"
            line = inFile.readline().decode().rstrip('\n').split('\t')
            if len(line) < columns:
                columns = len(line)
            for j in range(columns):
                if i == 0:
                    table += "\t\t<th align='left'>{}</th>\n".format(line[j])
                else:
                    table += "\t\t<td align='left'>{}</td>\n".format(line[j])

            table += '\n'
            table += '\t</tr>\n'
    table += '</table>\n'
    return table


# ----------------------------------------------------------------------------------------------------------
# import sys
#
# one_data = sys.argv[1]
# one_test = sys.argv[2]
# two_data = sys.argv[3]
# two_test = sys.argv[4]
# outFile = open(sys.argv[5], 'w')
#
# dataList, testList = check_test_for_every_data(sys.argv[1:])
#
# output = compare_files(dataList, testList)
# output += check_test_files(testList, 3, 3, 3)
#
#
# outFile.write(output)
#
# outFile.close()
