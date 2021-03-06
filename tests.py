import os, os.path, re, subprocess
from Constants import *
from PullRequest import PullRequest
from Shared import *
from yaml.scanner import ScannerError
from yaml import load
from yaml import FullLoader

def check_changed_files(changed_files, pr):
    report = ""
    valid = True

    for fileName in changed_files:
        path = fileName.split("/")

        if path[0] != pr.branch and path[0] != "Helper":
            valid = False
            report += "{0}\tOnly files in the \"{1}\" or \"Helper\" directory should be changed on this branch, but \"{2}\" was changed.".format(RED_X, pr.branch, fileName)

    pr.report.valid_files = valid

    if not valid:
        pr.report.valid_files_report = report
        pr.status = 'Failed'

    return valid

def test_folder(pr: PullRequest):
    printToLog('Testing - test_folder', pr)

    report = '### Testing Directory . . .\n\n'
    passed = True
    file_list = get_files(".")

    for path in file_list:
        if path[0] != '.':
            file_check_string = str(subprocess.check_output(['file', '-b', path]))

            if not re.search(r"ASCII", file_check_string) and not re.search(r"empty", file_check_string) and not \
                    re.search(r"script text", file_check_string) and not re.search(r"directory", file_check_string):
                report += "{0}\t{1} is not a text file.\n\n".format(RED_X, path)
                passed = False

        if os.path.getsize(path) > 1000000:
            report += RED_X + '\t' + path + ' is too large ( ' + str(int(os.path.getsize(path) / 1048576)) + 'MB; max size: 1MB)\n\n'
            passed = False

    invalid_file_list = [x for x in file_list if os.path.basename(x).endswith(".tsv") and not os.path.basename(x).startswith("test_")]
    if len(invalid_file_list) > 0:
        for f in invalid_file_list:
            report += '{0}\t A file called {1} is not allowed in the directory.\n\n'.format(RED_X, f)
        passed = False

    if passed:
        report += '#### Results: PASS\n---\n'
        printToLog('PASS - test_folder', pr)
    else:
        report += '#### Results: **<font color=\"red\">FAIL</font>**\n---\n'
        printToLog('FAIL - test_folder', pr)

    pr.report.directory_test_report = report
    pr.report.pass_directory_test = passed

    return passed

def test_config(pr: PullRequest):
    printToLog('Testing - test_config', pr)

    passed = True
    report = '### Testing Configuration File . . .\n\n'

    config_path = "./{}/{}".format(pr.branch, CONFIG_FILE_NAME)

    if os.path.exists(config_path):
        with open(config_path, 'r') as stream:
            try:
                configs = load(stream, Loader=FullLoader)
                for config in REQUIRED_CONFIGS:
                    if config not in configs.keys():
                        passed = False
                        report += RED_X + '\t' + CONFIG_FILE_NAME + ' does not contain a configuration' \
                                                                    ' for \"' + config + '\".\n\n'
                if passed:
                    report += CHECK_MARK + '\t' + CONFIG_FILE_NAME + ' contains all necessary configurations.\n\n'
                if 'title' in configs:
                    if len(configs['title']) > MAX_TITLE_SIZE:
                        passed = False
                        report += RED_X + '\tDataset Title cannot exceed ' + str(MAX_TITLE_SIZE) + ' characters.\n\n'
                    else:
                        report += CHECK_MARK + '\tTitle is less than ' + str(MAX_TITLE_SIZE) + ' characters\n\n'
            except ScannerError as e:
                passed = False
                report += '{}\tInvalid yaml, error (Likely due to invalid colons):\n\n{}\n\n'.format(RED_X, e)
    else:
        report += RED_X + '\t ' + CONFIG_FILE_NAME + ' does not exist\n\n'
        passed = False

    description_path = "./{}/{}".format(pr.branch, DESCRIPTION_FILE_NAME)
    if os.path.exists(description_path):
        with open(description_path, 'r') as description_file:
            if len(description_file.read()) < 10:
                passed = False
                report += RED_X + '\t' + DESCRIPTION_FILE_NAME + ' must contain a description of the dataset.\n\n'
            else:
                report += CHECK_MARK + '\t' + DESCRIPTION_FILE_NAME + ' contains a description.\n\n'
    else:
        report += RED_X + '\t' + DESCRIPTION_FILE_NAME + ' does not exist\n\n'
        passed = False

    if passed:
        report += '#### Results: PASS\n---\n'
        printToLog('PASS - test_config', pr)
    else:
        report += '#### Results: **<font color=\"red\">FAIL</font>**\n---\n'
        printToLog('FAIL - test_config', pr)

    pr.report.configuration_test_report = report
    pr.report.pass_configuration_test = passed

    return passed

def test_files(pr: PullRequest):
    printToLog('Testing - test_files', pr)

    passed = True
    report = '\n### Testing file paths:\n\n'
    report += '### Running install\n\n'

    for file_name in REQUIRED_FILES:
        path = "{}/{}/{}".format(os.getcwd(), pr.branch, file_name)

        if os.path.exists(path):
            report += CHECK_MARK + '\t' + file_name + ' exists.\n\n'
        else:
            report += RED_X + '\t' + file_name + ' does not exist.\n\n'
            passed = False

    if passed:
        report += '#### Results: PASS\n---\n'
        printToLog('PASS - test_files', pr)
    else:
        report += '#### Results: **<font color=\"red\">FAIL</font>**\n---\n'
        printToLog('FAIL - test_files', pr)

    pr.report.pass_file_test = passed
    pr.report.file_test_report = report

    return passed

# Are all test files in correct format?
def check_test_files(test_file_list, pr):
    printToLog("Running check_test_files", pr)

    report = ''
    passed = True

    for f in test_file_list:
#        min_features = MIN_FEATURES
#        data_file_path = get_data_file_path(f)
#        num_data_features = get_num_data_features(data_file_path)
#
#        if num_data_features == 1:
#            min_features = 1

        row_count = 0
        samples = {}

        report += "#### Running \"{0}\"\n\n".format(f)

        with open(f, 'r') as test_file:
            headers = test_file.readline().rstrip('\n').split('\t')
            # Make sure there are three columns named Sample, Variable, Value
            passed, temp_report = check_test_columns(headers, f, pr)
            if passed:
                report += "{check_mark}\t\"{0}\" has three columns with the correct headers\n\n"\
                    .format(os.path.basename(f), check_mark=CHECK_MARK)
            else:
                report += temp_report

            for line in test_file:
                row_count += 1
                data = line.rstrip('\n').split('\t')
                if len(data) is not 3 and len(data) is not 0:  # Make sure each row has exactly three columns
                    report += "{red_x}\tRow {0} of \"{1}\" should contain exactly three columns\n\n"\
                        .format(row_count, os.path.basename(f), red_x=RED_X)
                    passed = False
                elif len(data) != 0:  # Add data to a map
                    if data[0] not in samples.keys():
                        samples[data[0]] = [data[1] + data[2]]
                    else:
                        if data[1] + data[2] not in samples[data[0]]:
                            samples[data[0]].append(data[1] + data[2])

        if len(samples.keys()) < MIN_SAMPLES:  # Make sure there are enough unique sample IDs to test
            report += "{red_x}\t\"{0}\" does not contain enough unique samples to test (min: {1})\n\n"\
                .format(os.path.basename(f), MIN_SAMPLES, red_x=RED_X)
            passed = False
        else:
            report += "{check_mark}\t\"{0}\" contains enough unique samples to test\n\n"\
                .format(os.path.basename(f), check_mark=CHECK_MARK)

#        for sample in samples:  # Make sure each sample has enough features to test
#            if len(samples[sample]) < min_features:
#                report += "{red_x}\tSample \"{0}\" does not have enough features to test (min: {1})\n\n"\
#                    .format(sample, min_features, red_x=RED_X)
#                passed = False
#
#        if passed:
#            report += "{check_mark}\t\"{0}\" has enough features to test (min: {1}) for every sample\n\n"\
#                .format(os.path.basename(f), min_features, check_mark=CHECK_MARK)

        if row_count == 0:  # Check if file is empty
            report += "{red_x}\t\"{0}\" is empty.\n\n".format(f, red_x=RED_X)
            passed = False
        elif row_count < MIN_TEST_CASES:  # Check if there are enough test cases
            report += "{red_x}\t\"{0}\" does not contain enough test cases ({1}; min: {2})\n\n"\
                .format(os.path.basename(f), row_count, MIN_TEST_CASES, red_x=RED_X)
            passed = False
        else:
            report += "{check_mark}\t\"{0}\" contains enough test cases ({1}; min: {2})\n\n"\
                .format(os.path.basename(f), row_count, MIN_TEST_CASES, check_mark=CHECK_MARK)

#    r, passed = check_test_files(test_file_list, pr)
#    report += r

    if passed:
        report += "#### Results: PASS\n---\n"
    else:
        report += "#### Results: FAIL\n---\n"

    return report, passed

#def get_num_data_features(data_file_path):
#    printToLog("Checking how many variables are in {}".format(data_file_path), pr)
#
#    header_items = None
#    with gzip.open(data_file_path) as data_file:
#        header_items = data_file.readline().decode().rstrip("\n").split("\t")
#
#    return len(header_items)

# Check if the column headers of the test f are "Sample", "Variable", and "Value"
def check_test_columns(col_headers, file, pr):
    printToLog("Running check_test_columns", pr)
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

def test_scripts(pr: PullRequest):
    printToLog('Testing - test_scripts', pr)

    report = "### Running user scripts . . .\n\n"

    passed = True
    for script_name in USER_SCRIPTS:
        printToLog('Testing - test_bash_script - {}'.format(os.path.basename(script_name)), pr)
        command = "cd {}/{}; bash {} >> {}".format(os.getcwd(), pr.branch, script_name, pr.log_file_path)
        printToLog(command)
        return_code = execShellCommand(command, pr)
        printToLog("Return code: {}".format(return_code))

        if return_code != 0:
            report += '\n\n' + RED_X + '\t' + os.path.basename(script_name) + ' returned an error. Check the attached log.\n\n'
            passed = False
            printToLog('FAIL - test_bash_script - {}'.format(os.path.basename(script_name)), pr)
            break
        else:
            report += '\n\n' + CHECK_MARK + '\t' + os.path.basename(script_name) + ' executed properly.\n\n'
            printToLog('PASS - test_bash_script - {}'.format(os.path.basename(script_name)), pr)

    if passed:
        report += "#### Results: PASS\n---\n"
    else:
        report += "#### Results: **<font color=\"red\">FAIL</font>**\n---\n"

    pr.report.pass_script_test = passed
    pr.report.script_test_report = report

    return passed

def test_tsv(pr: PullRequest, tsv_file_paths):
    passed = True
    report = '\n### Testing tsv files:\n\n'

    printToLog('Testing - test_tsv', pr)

    if len(tsv_file_paths) == 0:
        report += RED_X + '\tNo tsv files exist.\n\n'
        passed = False
#    else:
#        for file_path in tsv_file_paths:
#            file_type = str(subprocess.check_output(['file', '-b', file_path]))
#
#            if re.search(r"gzip compressed data", file_type):
#                report += CHECK_MARK + '\t' + os.path.basename(file_path) + ' was created and zipped correctly.\n\n'
#            else:
#                report += RED_X + '\t' + os.path.basename(file_path) + ' exists, but was not zipped correctly (' + file_type.decode() + ').\n\n'
#                passed = False

    if passed:
        report += '#### Results: PASS\n---\n'
        printToLog('PASS - test_tsv', pr)
    else:
        report += '#### Results: **<font color=\"red\">FAIL</font>**\n---\n'
        printToLog('FAIL - test_tsv', pr)

    pr.report.pass_tsv_test = passed
    pr.report.tsv_test_report = report

    return passed
