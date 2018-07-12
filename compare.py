import gzip
from Constants import *
import pandas as pd


def one_feature(file):
    df = pd.read_csv(file, sep='\t')
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
            # Make sure there are three columns named SampleID, Variable, Value
            passed, temp_report = check_test_columns(headers, file)
            if passed:
                report += "{check_mark}\t\"{0}\" has three columns with the correct headers\n\n"\
                    .format(file, check_mark=CHECK_MARK)
            else:
                report += temp_report

            for line in test_file:
                row_count += 1
                data = line.rstrip('\n').split('\t')
                report += "number of columns is {}\n".format(len(data))
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
                report += "{red_x}\t\SampleID \"{0}\" does not have enough features to test (min: {1})\n\n"\
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
        report += "#### Results: PASS\n\n\n"
    else:
        report += "#### Results: FAIL\n\n\n"

    return report, passed


# Check if the column headers of the test file are "SampleID", "Variable", and "Value"
def check_test_columns(col_headers, file):
    passed = True
    report = ""

    if len(col_headers) != 3:  # Make sure there are exactly three columns
        report += "{red_x}\t\"{0}\" does not contain three columns\n\n".format(file, red_x=RED_X)
        passed = False
    else:  # Check the names of each column
        if col_headers[0] != "SampleID":
            report += "{red_x}\tFirst column of \"{0}\" must be titled \"SampleID\"\n\n'".format(file, red_x=RED_X)
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
    report = "#### Comparing Files:\n\n"

    for file in data_file_list:

        passed = True
        column_headers = []  # List of column headers AKA feature names
        data_row_count = 0  # Row count in data file
        test_samples = []  # sample IDs in test file
        data_samples = set()  # Set of sample IDs in data file
        test_row_count = 0  # Row count in test file
        test_file_name = ""
        test_dict = {}
        tested_rows = []

        with gzip.open(file, 'r') as data_file:
            # ----------------------------------------------------------------------------------------------------------
            for test in test_file_list:  # Get matching test file
                if file.rstrip('.gz') == test.lstrip('test_'):
                    test_file_name = test

            # PARSING THROUGH TEST FILE
            with open(test_file_name, 'r') as test_file:
                test_file.readline().rstrip('\n').split('\t')
                for line in test_file:
                    test_row_count += 1
                    test_data = line.rstrip('\n').split('\t')
                    test_data.append(test_row_count)  # Add row number to the list
                    test_samples.append(test_data[0])  # Add sample IDs in test file to the list
                    if test_data[0] in test_dict.keys():  # Create a map {sample: list of variable+value+row}
                        test_dict[test_data[0]].extend([test_data])
                    else:
                        test_dict.setdefault(test_data[0], []).extend([test_data])
            # ----------------------------------------------------------------------------------------------------------

            report += "#### Running \"{0}\" and \"{1}\"\n\n".format(file, test_file_name)

            data_headers = data_file.readline().decode().rstrip('\n').split('\t')

            for variable in data_headers:  # Make sure column headers are unique in data file
                if variable not in column_headers:
                    column_headers.append(variable)
                else:
                    passed = False
                    report += "{red_x}\t{0} is in \"{1}\" column headers more than once\n\n"\
                        .format(variable, file, red_x=RED_X)

            if data_headers[0] != "SampleID":  # Make sure first column header is named "SampleID"
                report += "{red_x}\tFirst column of \"{0}\" must be titled \"SampleID\"\n\n".format(file, red_x=RED_X)
            else:
                report += "{check_mark}\tFirst column of \"{0}\" is titled \"SampleID\"\n\n"\
                    .format(file, check_mark=CHECK_MARK)

            # PARSING THROUGH DATA FILE
            for line in data_file:
                data_row_count += 1
                data = line.decode().rstrip('\n').split('\t')
                data_samples.add(data[0])  # Add sample IDs in data file to the set
                if data[0] in test_dict.keys():
                    for info in test_dict[data[0]]:
                        row = info[3]  # row number from test file
                        tested_rows.append(row)  # Keep track of rows checked
                        if info[1] in data_headers:  # Check if variable in test file is in data file
                            variable_index = data_headers.index(info[1])
                            if info[2] == data[variable_index]:  # Check if variable's value in test file matches value in data file
                                report += "{check_mark}\tRow {0}: Success\n\n".format(row, check_mark=CHECK_MARK)
                            else:
                                passed = False
                                report += "{red_x}\tRow {0}: Fail - Values do not match\n\n".format(row, red_x=RED_X)
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
    if passed_all:
        report += "#### Results: TRUE\n\n\n"
    else:
        report += "#### Results: FAIL\n\n\n"
    return report, passed_all


# Check if there is a test file for every data file
def check_test_for_every_data(pr, file_list):
    report = "### Testing Test Files:\n\n"
    data_files = []
    test_files = []
    bad_data_files = []
    bad_test_files = []
    passed = True

    for file in file_list:
        if file.endswith(".gz"):
            data_files.append(file)
            bad_data_files.append(file)
        elif "test" in file:
            test_files.append(file)
            bad_test_files.append(file)

    for data in data_files:
        for test in test_files:
            if data.rstrip('.gz') == test.lstrip('test_'):
                bad_data_files.remove(data)
                bad_test_files.remove(test)

    if len(bad_data_files) + len(bad_test_files) > 0:
        passed = False

    for file in data_files:
        convert(file)

    if not passed:
        for file in bad_data_files:
            report += "{}\tData file {} is missing required test file \"test{}.tsv\"\n\n"\
                .format(RED_X, file, file.rstrip('.tsv.gz'))
        for file in bad_test_files:
            report += "{}\tTest file {} is missing required data file \"{}.gz\"\n\n"\
                .format(RED_X, file, file.lstrip('test_'))
        report += "#### Results: FAIL\n\n\n"
    else:
        r, passed = check_test_files(test_files)
        report += r
        if passed:
            r, passed = compare_files(data_files, test_files)
            report += r
    pr.report.key_test_report = report
    pr.report.pass_key_test = passed
    return passed


def convert(file):
    if 'metadata' in file:
        df = pd.read_csv(file, sep='\t')
        if len(df.columns.values) == 3:
            if 'Variable' in df.columns.values and 'Value' in df.columns.values:
                metadata = {}
                with gzip.open(file) as fp:
                    fp.readline()
                    for line in fp:
                        data = line.decode().rstrip('\n').split('\t')
                        metadata.setdefault(data[0], {})[data[1]] = data[2]
                df = pd.DataFrame(metadata).T
                df.to_csv(file, compression='gzip', sep='\t', index_label='SampleID')


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
