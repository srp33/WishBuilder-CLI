from capturer import CaptureOutput
from compare import *
import glob
import inspect
from multiprocessing import Process
import os
import psutil
import shutil
import sys
from tests import *
import time
from Constants import *
from DataSetBuilder import *
from GithubDao import GithubDao
from Shared import *
from SqliteDao import SqliteDao

def setup():
    os.chdir(WB_DIRECTORY)
    required_directories = [RAW_DATA_STORAGE, GENEY_DATA_LOCATION, TESTING_LOCATION]

    for path in required_directories:
        if not os.path.exists(path):
            os.makedirs(path)

def get_new_prs(sql_dao, git_dao):
    full_history = sql_dao.get_all()
    prs = git_dao.get_prs()
    if not full_history:
        return prs

    new_pulls = []
    for pr in prs:
        if pr.pr in full_history.keys():
            if pr.sha not in full_history[pr.pr]:
                new_pulls.append(pr)
        else:
            new_pulls.append(pr)

    return new_pulls

def get_exception_stack(e):
    error = ""

    tb = sys.exc_info()[2]

    error += 'Traceback (most recent call last):'
    for item in reversed(inspect.getouterframes(tb.tb_frame)[1:]):
        if item == None:
            continue

        error += ' File "{1}", line {2}, in {3}\n<br><br>\n'.format(*item)
        for line in item[4]:
            error += ' ' + line.lstrip()
        for item in inspect.getinnerframes(tb):
            error += ' File "{1}", line {2}, in {3}\n<br><br>\n'.format(*item)

        if len(item) < 5 or item[4] == None:
            continue
        for line in item[4]:
            error += ' ' + line.lstrip()

    error += "<br>\n<br><b>" + str(e) + "</b>"

    return error

def test(pr: PullRequest, sql_dao):
    cwd = os.getcwd()

    try:
        test_dir = os.path.join(TESTING_LOCATION, pr.branch)
        raw_data_storage = os.path.join(RAW_DATA_STORAGE, pr.branch)

        shutil.rmtree(test_dir, ignore_errors=True)
        os.mkdir(test_dir)

        printToLog("Testing {}, Pull Request #{}...".format(pr.branch, pr.pr), pr)

        pr.status = 'In progress'
        pr.email = git_dao.get_email(pr.sha)
        pr.log_file_path = os.path.join(test_dir, LOG_FILE_NAME)
        sql_dao.update(pr)

        start = time.time()

        passed = check_changed_files(git_dao.get_files_changed(pr), pr)

        if passed:
            git_dao.get_branch(pr, test_dir)

            os.chdir(test_dir)

            data_dir = "{}/{}".format(test_dir, pr.branch)
            test_file_paths = [data_dir + "/" + x for x in os.listdir(data_dir) if x.startswith("test_") and x.endswith(".tsv")]

            if len(test_file_paths) == 0:
                passed = False
                printToLog("No test files exist.")

            # Run tests
            if not test_folder(pr):
                pr.passed = False
            if not test_config(pr):
                pr.passed = False
            if not test_files(pr):
                pr.passed = False
            if not check_test_files(test_file_paths, pr):
                pr.passed = False

            # if this test doesn't pass, it is pointless to move on, because the output files will be wrong
            if test_scripts(pr):
                tsv_file_paths = [data_dir + "/" + x for x in os.listdir(data_dir) if not x.startswith("test_") and  x.endswith(".tsv")]

                tsv_passed = test_tsv(pr, tsv_file_paths)
                data_passed = check_test_for_every_data(pr, tsv_file_paths, test_file_paths)

                if tsv_passed and data_passed:
                    raw_data_storage = os.path.join(RAW_DATA_STORAGE, pr.branch)
                    shutil.rmtree(raw_data_storage, ignore_errors=True)
                    os.mkdir(raw_data_storage)

                    for f in tsv_file_paths:
                        os.system('mv {} {}/'.format(f, raw_data_storage))

                        if os.path.exists("{}.aliases".format(f)):
                            os.system("mv {}.aliases {}/".format(f, raw_data_storage))
            else:
                pr.passed = False

            printToLog("Updating status", pr)
            pr.time_elapsed = time.strftime("%Hh:%Mm:%Ss", time.gmtime(time.time() - start))
            pr.date = time.strftime("%D", time.gmtime(time.time()))
            pr.e_date = time.time()
            pr.check_if_passed()
            sql_dao.update(pr)

            if pr.passed:
                if build_geney_files(pr, test_dir, raw_data_storage):
                    printToLog("Successfully built Geney files", pr)
                    git_dao.merge(pr)
                else:
                    printToLog("Failed to build Geney files", pr)
                    pr.passed = False

    except Exception as e:
        pr.status = 'Error'
        pr.passed = False
        pr.report.other = True
        exception_stack = get_exception_stack(e)
        pr.report.other_content = exception_stack
        print("Exception for pull request #{}".format(pr.branch))
        print(exception_stack)

    send_report(pr)
    os.chdir(cwd)
    shutil.rmtree(test_dir, ignore_errors=True)
    shutil.rmtree(raw_data_storage, ignore_errors=True)
    print("Done")

def build_geney_files(pr: PullRequest, test_dir, raw_data_storage):
    printToLog("Building files for use in Geney", pr)

    cwd = os.getcwd()
    os.chdir(raw_data_storage)

    geney_dataset_path = os.path.join(GENEY_DATA_LOCATION, pr.branch)
    shutil.rmtree(geney_dataset_path, ignore_errors=True)
    os.mkdir(geney_dataset_path)

    tsv_files = glob.glob("*.tsv")

    if len(tsv_files) == 0:
        printToLog("No .tsv file could be found in {}.".format(raw_data_storage), pr)
        return False

    fwf_files = []

    for tsv_file in tsv_files:
        fwf_file = tsv_file.replace(parse_file_ext(tsv_file), ".fwf")
        fwf_files.append(fwf_file)

        printToLog("Creating fixed-width file for {}".format(tsv_file), pr)
        convert_tsv_to_fwf(tsv_file, fwf_file)
        printToLog("Done creating fixed-width file for {}".format(tsv_file), pr)

    out_data_file_path = os.path.join(geney_dataset_path, "data.fwf")

    if len(fwf_files) == 1:
        build_metadata(os.path.join(test_dir, pr.branch), fwf_files[0])

        os.system("mv {} {}".format(fwf_files[0], out_data_file_path))
        for f in glob.glob("{}.*".format(fwf_files[0])):
            os.system("mv {} {}{}".format(f, out_data_file_path, parse_file_ext(f)))
    else:
        printToLog("Creating merged file {} from {}".format(out_data_file_path, " and ".join(fwf_files)), pr)
        merge_fwf_files(fwf_files, out_data_file_path)
        build_metadata(os.path.join(test_dir, pr.branch), out_data_file_path)
        printToLog("Done creating merged file {} from {}".format(out_data_file_path, " and ".join(fwf_files)), pr)

    os.chdir(cwd)

    printToLog("Setting permissions on {}".format(geney_dataset_path), pr)
    os.system("chmod 777 {} -R".format(geney_dataset_path))

    return True

def send_report(pr):
    pr.send_report(WISHBUILDER_EMAIL, WISHBUILDER_PASS, send_to='stephen.piccolo.byu@gmail.com')

    try:
        pr.send_report(WISHBUILDER_EMAIL, WISHBUILDER_PASS)
    except Exception as e:
        printToLog(get_exception_stack(e), pr)

    printToLog("Sent email report", pr)

if __name__ == '__main__':
    with CaptureOutput() as capturer:
        GH_TOKEN = os.environ['GH_TOKEN']
        WISHBUILDER_EMAIL = os.environ['WISHBUILDER_EMAIL']
        WISHBUILDER_PASS = os.environ['WISHBUILDER_PASS']
        SLEEP_SECONDS = int(os.environ['SLEEP_SECONDS'])

        setup()
        sql_dao = SqliteDao(SQLITE_FILE)
        git_dao = GithubDao('https://api.github.com/repos/srp33/WishBuilder/', GH_TOKEN)

        if os.path.exists(PRS_TO_TEST_FILE):
            with open(PRS_TO_TEST_FILE) as prFile:
                for line in prFile:
                    if line.startswith("#"):
                        continue

                    pr = line.rstrip()
                    printToLog("Removing pull request #{} from database.".format(pr))
                    sql_dao.remove_pr(pr)

        processes = []
        queue = []
        history = []
        while True:
            printToLog("Check for prs")
            new_prs = get_new_prs(sql_dao, git_dao)
            for pull in new_prs:
                if pull.sha not in history:
                    queue.append(pull)

            while len(queue) > 0:
                for p in processes:
                    if not p.is_alive():
                        processes.remove(p)
                    else:
                        if psutil.Process(p.pid).memory_info().rss > 3e+10:
                            printToLog('Memory limit reached!')
                            p.terminate()
                            break

                if len(processes) < MAX_NUM_PROCESSES:
                    new_pr = queue.pop()
                    history.append(new_pr.sha)
                    p = Process(target=test, args=(new_pr, sql_dao))
                    processes.append(p)

                    p.start()

                time.sleep(5)

            time.sleep(SLEEP_SECONDS)
