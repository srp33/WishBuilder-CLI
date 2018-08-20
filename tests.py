import gzip, os, os.path, re, subprocess, yaml
from yaml.scanner import ScannerError
from Constants import *
from PullRequest import PullRequest
from Shared import *

def check_files_changed(pr: PullRequest, files):
    bad_files = []
    report = ""
    valid = True
    for fileName in files:
        path = fileName.split("/")
        if path[0] != pr.branch and path[0] != 'Helper':
            bad_files.append(fileName)
    if len(bad_files) > 0:
        valid = False
        report += "Only files in the \"{}\" or \"Helper\" directory should be changed. The following files were also " \
                  "changed in this branch:\n".format(pr.branch)
        for file in bad_files:
            report += "- {}\n".format(file)
        pr.report.valid_files = False
        pr.report.valid_files_report = report
    if not valid:
        pr.report.valid_files = False
        pr.report.valid_files_report = report
        pr.status = 'Failed'
    for fileName in files:
        if fileName != "{}/description.md".format(pr.branch) and fileName != "{}/config.yaml".format(pr.branch):
            return valid, False
    return valid, True

def listdir_fullpath(directory: str) -> []:
    return [os.path.join(directory, file) for file in os.listdir(directory)]

def get_files(directory: str) -> []:
    files = []
    file_list = listdir_fullpath(directory)
    for file in file_list:
        if os.path.isdir(file):
            files.extend(get_files(file))
        else:
            files.append(file)
    return files

def test_folder(pr: PullRequest):
    printToLog('Testing - test_folder', pr)

    report = '### Testing Directory . . .\n\n'
    passed = True
    file_list = get_files(os.path.join(TESTING_LOCATION, pr.branch))

    for path in file_list:
        if path[0] != '.':
            # path = "{}{}/{}".format(TESTING_LOCATION, pr.branch, path)
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
    config_path = "{}{}/{}".format(TESTING_LOCATION, pr.branch, CONFIG_FILE_NAME)
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
    description_path = "{}{}/{}".format(TESTING_LOCATION, pr.branch, DESCRIPTION_FILE_NAME)
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
    script = os.path.join(TESTING_LOCATION, pr.branch, INSTALL_FILE_NAME)
    result, successful = test_bash_script(script, pr)
    report += result
    if not successful:
        passed = False
    for path in REQUIRED_FILES:
        if os.path.exists(path):
            report += CHECK_MARK + '\t' + path + ' exists.\n\n'
        else:
            report += RED_X + '\t' + path + ' does not exist.\n\n'
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
    for script in USER_SCRIPTS:
        script = os.path.join(TESTING_LOCATION, pr.branch, script)
        result, successful = test_bash_script(script, pr)
        report += result
        if not successful:
            passed = False
            break
    result, successful = check_zip()
    report += result
    if not successful:
        passed = False
    if passed:
        report += "#### Results: PASS\n---\n"
    else:
        report += "#### Results: **<font color=\"red\">FAIL</font>**\n---\n"

    pr.report.pass_script_test = passed
    pr.report.script_test_report = report

    return passed

def test_bash_script(bash_script_name, pr: PullRequest):
    printToLog('Testing - test_bash_script - {}'.format(os.path.basename(bash_script_name)), pr)

    report = "Executing " + bash_script_name + ": "
    passed = True
    # os.system('bash {}'.format(bash_script_name))
    results = subprocess.run(
        'bash {} >> {}'.format(bash_script_name, pr.log_file_path), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    if results.returncode != 0:
        report += '\n\n' + RED_X + '\t' + bash_script_name.split('/')[-1] + ' returned an error:\n```bash\n' + \
                     results.stderr.decode().rstrip('\n') + '\n```\n\n'
        passed = False
        printToLog('FAIL - test_bash_script - {}'.format(os.path.basename(bash_script_name)), pr)
    else:
        report += "Success\n\n"
        printToLog('PASS - test_bash_script - {}'.format(os.path.basename(bash_script_name)), pr)
    return report, passed

def check_zip():
    passed = True
    report = ""
    for path in os.listdir('./'):
        if path.endswith('.gz'):
            file_type = str(subprocess.check_output(['file', '-b', path]))
            if re.search(r"gzip compressed data", file_type):
                report += CHECK_MARK + '\t' + path + ' was created and zipped correctly.\n\n'
            else:
                report += RED_X + '\t' + path + ' exists, but was not zipped correctly.\n\n'
                passed = False
    return report, passed

def test_cleanup(original_directory, pr: PullRequest):
    printToLog('Testing - test_cleanup', pr)

    os.system('chmod +x ./' + CLEANUP_FILE_NAME)
    os.system('./' + CLEANUP_FILE_NAME)

    passed = True
    report = '### Testing Directory after cleanup . . .\n\n'
    current_directory = os.listdir(os.getcwd())

    for file in current_directory:
        if file not in original_directory:
            passed = False
            report += RED_X + '\t\"' + file + '\" should have been deleted during cleanup.\n\n'

    if passed:
        report += '#### Results: PASS\n---\n'
        printToLog('PASS - test_cleanup', pr)
    else:
        report += '#### Results: **<font color=\"red\">FAIL</font>**\n---\n'
        printToLog('FAIL - test_cleanup', pr)

    pr.report.cleanup_report = report
    pr.report.pass_cleanup = passed
    return passed
