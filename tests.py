import gzip, os, os.path, re, subprocess, yaml
from yaml.scanner import ScannerError
from Constants import *
from PullRequest import PullRequest
from Shared import *

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

    invalid_file_list = [x for x in file_list if x.endswith(".tsv.gz") or x == ".gitignore"]
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
                configs = yaml.load(stream)
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

def test_scripts(pr: PullRequest):
    printToLog('Testing - test_scripts', pr)

    report = "### Running user scripts . . .\n\n"

    passed = True
    for script_name in USER_SCRIPTS:
        printToLog('Testing - test_bash_script - {}'.format(os.path.basename(script_name)), pr)
        command = "cd {}/{}; bash {} >> {}".format(os.getcwd(), pr.branch, script_name, pr.log_file_path)
        printToLog(command)
        return_code = execShellCommand(command)
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

def test_gzip(pr: PullRequest, gz_file_paths):
    passed = True
    report = '\n### Testing gzip files:\n\n'

    printToLog('Testing - test_gzip', pr)

    if len(gz_file_paths) == 0:
        report += RED_X + '\tNo gzip files exist.\n\n'
        passed = False
    else:
        for file_path in gz_file_paths:
            file_type = str(subprocess.check_output(['file', '-b', file_path]))

            if re.search(r"gzip compressed data", file_type):
                report += CHECK_MARK + '\t' + os.path.basename(file_path) + ' was created and zipped correctly.\n\n'
            else:
                report += RED_X + '\t' + os.path.basename(file_path) + ' exists, but was not zipped correctly (' + file_type.decode() + ').\n\n'
                passed = False

    if passed:
        report += '#### Results: PASS\n---\n'
        printToLog('PASS - test_gzip', pr)
    else:
        report += '#### Results: **<font color=\"red\">FAIL</font>**\n---\n'
        printToLog('FAIL - test_gzip', pr)

    pr.report.pass_gzip_test = passed
    pr.report.gzip_test_report = report

    return passed
