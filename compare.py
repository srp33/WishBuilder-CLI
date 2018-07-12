import gzip


# Are all test files in correct format?
def check_test_files(test_file_list, min_samples, min_features, min_test_cases):
    report = "### Testing Test Files:\n\n"
    passed = True

    for file in test_file_list:

        row_count = 0
        samples = {}

        report += "#### Running \"{0}\"\n\n".format(file)

        with open(file, 'r') as test_file:
            headers = test_file.readline().rstrip('\n').split('\t')
            # Make sure there are three columns named Sample, Variable, Value
            passed, temp_report = check_test_columns(headers, file)
            if passed:
                report += "CHECK_MARK\t\"{0}\" has three columns with the correct headers\n\n".format(file)
            else:
                report += temp_report

            for line in test_file:
                row_count += 1
                data = line.rstrip('\n').split('\t')
                report += "number of columns is {}\n".format(len(data))
                if len(data) is not 3 and len(data) is not 0:  # Make sure each row has exactly three columns
                    report += "RED_X\tRow {0} of \"{1}\" should contain exactly three columns\n\n".format(row_count, file)
                    passed = False
                elif len(data) != 0:  # Add data to a map
                    if data[0] not in samples.keys():
                        samples[data[0]] = data[1] + data[2]
                    else:
                        if data[1] + data[2] not in samples[data[0]]:
                            samples[data[0]].append(data[1]+data[2])

        if len(samples.keys()) < min_samples:  # Make sure there are enough unique sample IDs to test
            report += "RED_X\t\"{0}\" does not contain enough unique samples to test (min: {1})\n\n".format(file, min_samples)
            passed = False
        else:
            report += "CHECK_MARK\t\"{0}\" contains enough unique samples to test\n\n".format(file)

        for sample in samples:  # Make sure each sample has enough features to test
            if len(samples[sample]) < min_features:
                report += "RED_X\t\Sample \"{0}\" does not have enough features to test (min: {1})\n\n".format(sample, min_features)
                passed = False

        if passed:
            report += "CHECK_MARK\t\"{0}\" has enough features to test (min: {1})\n\n".format(file, min_features)

        if row_count == 0:  # Check if file is empty
            report += "RED_X\t\"{0}\" is empty.\n\n".format(file)
            passed = False
        elif row_count < min_test_cases:  # Check if there are enough test cases
            report += "RED_X\t\"{0}\" does not contain enough test cases ({1}; min: {2})\n\n".format(file, row_count, min_test_cases)
            passed = False
        else:
            report += "CHECK_MARK\t\"{0}\" contains enough test cases ({1}; min: {2})\n\n".format(file, row_count, min_test_cases)

    if passed:
        report += "#### Results: PASS\n\n\n"
    else:
        report += "#### Results: FAIL\n\n\n"

    return report


# Check if the column headers of the test file are "Sample", "Variable", and "Value"
def check_test_columns(col_headers, file):
    passed = True
    report = ""

    if len(col_headers) != 3:  # Make sure there are exactly three columns
        report += "RED_X\t\"{0}\" does not contain three columns\n\n".format(file)
        passed = False
    else:  # Check the names of each column
        if col_headers[0] != "Sample":
            report += "RED_X\tFirst column of \"{0}\" must be titled \"Sample\"\n\n'".format(file)
            passed = False
        if col_headers[1] != "Variable":
            report += "RED_X\tSecond column of \"{0}\" must be titled \"Variable\"\n\n".format(file)
            passed = False
        if col_headers[2] != "Value":
            report += "RED_X\tThird column of \"{0}\" must be titled \"Value\"\n\n')".format(file)
            passed = False

    return passed, report


def compare_files(data_file_list, test_file_list):
    report = "### Comparing Files:\n\n"

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
                if file[:-12] == test[:-9]:
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
                    report += "RED_X\t{0} is in \"{1}\" column headers more than once\n\n".format(variable, file)

            if data_headers[0] != "Sample":  # Make sure first column header is named "Sample"
                report += "RED_X\tFirst column of \"{0}\" must be titled \"Sample\"\n\n".format(file)
            else:
                report += "CHECK_MARK\tFirst column of \"{0}\" is titled \"Sample\"\n\n".format(file)

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
                                report += "CHECK_MARK\tRow {0}: Success\n\n".format(row)
                            else:
                                passed = False
                                report += "RED_X\tRow {0}: Fail - Values do not match\n\n".format(row)
                        else:  # Make sure
                            report += "RED_X\tRow {0} - Variable \"{1}\" is not found in \"{2}\" column headers\n\n".format(row, info[1], file)
                            passed = False

            if len(tested_rows) < test_row_count:
                passed = False
                for i in range(test_row_count):
                    if i + 1 not in tested_rows:
                        report += "RED_X\tRow {0} - Sample \"{1}\" from {2} is not found in \"{3}\"\n\n".format(i + 1, test_samples[i], test_file_name, file)

            if passed:
                report += "#### Results: PASS\n\n\n"
            else:
                report += "#### Results: FAIL\n\n\n"

    return report


# Check if there is a test file for every data file
def check_test_for_every_data(file_list):
    all_files = file_list[:]
    data_files = []
    test_files = []
    passed = True

    for file in file_list:
        if file.endswith("_data.tsv.gz"):
            data_files.append(file)
        elif file.endswith("_test.tsv"):
            test_files.append(file)

    for data in data_files:
        for test in test_files:
            if data[:-12] == test[:-9]:
                all_files.remove(data)
                all_files.remove(test)

    if len(all_files) > 0:
        passed = False

    return data_files, test_files


# ----------------------------------------------------------------------------------------------------------
import sys

one_data = sys.argv[1]
one_test = sys.argv[2]
two_data = sys.argv[3]
two_test = sys.argv[4]
outFile = open(sys.argv[5], 'w')

dataList, testList = check_test_for_every_data(sys.argv[1:])

output = compare_files(dataList, testList)
output += check_test_files(testList, 3, 3, 3)


outFile.write(output)

outFile.close()

