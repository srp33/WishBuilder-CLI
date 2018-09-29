import glob, json, logging, os, pickle, psutil, shutil, sys, time, inspect
from compare import *
from multiprocessing import Process
from GithubDao import GithubDao
from SqliteDao import SqliteDao
sys.path.insert(0, '/ShapeShifter')
import ShapeShifter
from tests import *
from Constants import *
from Shared import *
from capturer import CaptureOutput
from FastFileHelper import *

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

        shutil.rmtree(test_dir, ignore_errors=True)
        os.mkdir(test_dir)

        printToLog("Testing {}, Pull Request #{}...".format(pr.branch, pr.pr), pr)

        pr.status = 'In progress'
        pr.email = git_dao.get_email(pr.sha)
        pr.log_file_path = os.path.join(test_dir, LOG_FILE_NAME)
        sql_dao.update(pr)

        start = time.time()
        raw_data_storage = os.path.join(RAW_DATA_STORAGE, pr.branch)

        if not os.path.exists(test_dir):
            os.makedirs(test_dir)

        passed = check_changed_files(git_dao.get_files_changed(pr), pr)

        if passed:
            git_dao.get_branch(pr, test_dir)

            os.chdir(test_dir)

            data_dir = "{}/{}".format(test_dir, pr.branch)
            test_file_paths = [data_dir + "/" + x for x in os.listdir(data_dir) if x.startswith("test_") and x.endswith(".tsv")]
            print(test_file_paths)
            sys.exit(1)

            if len(test_file_paths) == 0:
                passed = False
                printToLog("No test files exist.")

            # Run tests
            test_folder(pr)
            test_config(pr)
            test_files(pr)
            check_test_files(test_file_paths, pr)

            # if this test doesn't pass, it is pointless to move on, because the output files will be wrong
            if test_scripts(pr):
                tsv_file_paths = [data_dir + "/" + x for x in os.listdir(data_dir) if x.endswith(".tsv")]

                tsv_passed = test_tsv(pr, tsv_file_paths)
                data_passed = check_test_for_every_data(pr, tsv_file_paths, test_file_paths)

                if tsv_passed and data_passed:
                    shutil.rmtree(raw_data_storage, ignore_errors=True)
                    os.mkdir(raw_data_storage)

                    for f in tsv_file_paths:
                        os.system('mv {} {}/'.format(f, raw_data_storage))

            pr.time_elapsed = time.strftime("%Hh:%Mm:%Ss", time.gmtime(time.time() - start))
            pr.date = time.strftime("%D", time.gmtime(time.time()))
            pr.e_date = time.time()
            pr.check_if_passed()
            sql_dao.update(pr)

            if pr.passed:
                if build_geney_files(pr, test_dir, raw_data_storage):
                    git_dao.merge(pr)
                else:
                    printToLog("Failed to build Geney files.")

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

#def convert_to_parquet(pr: PullRequest, test_dir, raw_data_storage):
#    printToLog("Building parquet file(s)", pr)
#
#    cwd = os.getcwd()
#    os.chdir(raw_data_storage)
#
#    geney_dataset_path = os.path.join(GENEY_DATA_LOCATION, pr.branch)
#    shutil.rmtree(geney_dataset_path, ignore_errors=True)
#    os.mkdir(geney_dataset_path)
#    data_files = os.listdir('./')
#
#    groups = {}
#    for f in data_files:
#        group_name = f.rstrip('.gz').rstrip('.tsv')
#        with gzip.open(f) as fp:
#            with gzip.open('tmp.tsv.gz', 'w') as fp_out:
#                columns = fp.readline().decode().rstrip('\n').split('\t')
#                groups[group_name] = [columns[0]]
#                for column in columns[1:]:
#                    option = '{}_{}'.format(group_name, column)
#                    groups[group_name].append(option)
#                fp_out.write('\t'.join(groups[group_name]).encode())
#                fp_out.write('\n'.encode())
#                for line in fp:
#                    fp_out.write(line)
#                groups[group_name].remove(columns[0])
#        os.remove(f)
#        shutil.move('tmp.tsv.gz', f)
#
#    num_features = 0
#    for group in groups:
#        num_features += len(groups[group])
#
#    pr.feature_variables = num_features
#    with open(os.path.join(geney_dataset_path, 'groups.json'), 'w') as fp_groups:
#        json.dump(groups, fp_groups)
#
#    data_path = os.path.join(geney_dataset_path, 'data.pq')
#    ss = ShapeShifter.ShapeShifter(data_files[0])
#    ss.merge_files(data_files[1:], data_path, 'parquet')
#
#    get_metadata(data_path, os.path.join(geney_dataset_path, 'metadata.pkl'))
#
#    get_description(pr, test_dir, os.path.join(geney_dataset_path, 'description.json'))
#
#    os.chdir(cwd)

def build_geney_files(pr: PullRequest, test_dir, raw_data_storage):
    printToLog("Building files for use in Geney", pr)

    cwd = os.getcwd()
    os.chdir(raw_data_storage)

    geney_dataset_path = os.path.join(GENEY_DATA_LOCATION, pr.branch)
    #TODO: Uncomment this and remove if clause
    #shutil.rmtree(geney_dataset_path, ignore_errors=True)
    if not os.path.exists(geney_dataset_path):
        os.mkdir(geney_dataset_path)

    tsv_files = glob.glob("*.tsv")

    if len(tsv_files) == 0:
        printToLog("No .tsv file could be found in {}.".format(raw_data_storage))
        return False

    prefixes = []
    tsv_map_dirs = []

    for tsv_file in tsv_files:
        prefix = tsv_file.replace(".tsv", "")
        tsv_map_dir = "{}.mp".format(prefix)

        shutil.rmtree(tsv_map_dir, ignore_errors=True)
        map_tsv(tsv_file, tsv_map_dir)
        tsv_map_dirs.append(tsv_map_dir)

    merged_file = os.path.join(geney_dataset_path, "data.tsv")
    merge_tsv(tsv_files, tsv_map_dirs, prefixes, merged_file, 50000)

#    for i in range(len(tsv_files)):
#        os.remove(tsv_files[i])
#        shutil.rmtree(tsv_map_dirs[i], ignore_errors=True)

    merged_map_dir = os.path.join(geney_dataset_path, "data.mp")
    map_tsv(merged_file, merged_map_dir)

    merged_transposed_file = os.path.join(geney_dataset_path, "transposed.tsv")
    merged_transposed_temp_dir = os.path.join(geney_dataset_path, "transposed.temp")

    transpose_tsv(merged_file, merged_map_dir, merged_transposed_file, merged_transpose_temp_dir, False, False, 500000000)

    merged_transposed_map_dir = os.path.join(geney_dataset_path, "transposed.mp")
    map_tsv(transposed_tsv_file, transposed_tsv_map_dir)

#    for f in data_files:
#        group_name = f.rstrip('.tsv')
#        with gzip.open(f) as fp:
#            with gzip.open('tmp.tsv.gz', 'w') as fp_out:
#                columns = fp.readline().decode().rstrip('\n').split('\t')
#                groups[group_name] = [columns[0]]
#                for column in columns[1:]:
#                    option = '{}_{}'.format(group_name, column)
#                    groups[group_name].append(option)
#                fp_out.write('\t'.join(groups[group_name]).encode())
#                fp_out.write('\n'.encode())
#                for line in fp:
#                    fp_out.write(line)
#                groups[group_name].remove(columns[0])
#        os.remove(f)
#        shutil.move('tmp.tsv.gz', f)

#    num_features = 0
#    for group in groups:
#        num_features += len(groups[group])

#    pr.feature_variables = num_features
#    with open(os.path.join(geney_dataset_path, 'groups.json'), 'w') as fp_groups:
#        json.dump(groups, fp_groups)

#    data_path = os.path.join(geney_dataset_path, 'data.pq')
#    ss = ShapeShifter.ShapeShifter(data_files[0])
#    ss.merge_files(data_files[1:], data_path, 'parquet')

#    get_metadata(data_path, os.path.join(geney_dataset_path, 'metadata.pkl'))

#    get_description(pr, test_dir, os.path.join(geney_dataset_path, 'description.json'))

    os.chdir(cwd)

    #TODO: This is temporary
    printToLog("Successfully saved Geney files")
    return False

def get_metadata(data_file, out_file):
    ss = ShapeShifter.ShapeShifter(data_file)
    column_dict = ss.get_all_columns_info()
    meta = {}

    for column in column_dict.keys():
        column_info = column_dict[column]
        if column_info.dataType == 'continuous':
            meta[column_info.name] = {
                'options': 'continuous',
                'min': min(column_info.uniqueValues),
                'max': max(column_info.uniqueValues)
            }
        else:
            options = column_info.uniqueValues
            num_options = len(options)
            meta[column_info.name] = {
                'numOptions': num_options,
                'options': options
            }

    metadata = {'meta': meta}
    with open(out_file, 'wb') as fp:
        pickle.dump(metadata, fp)

def get_description(pr: PullRequest, test_dir, out_file):
    with open(os.path.join(test_dir, pr.branch, CONFIG_FILE_NAME)) as config_fp:
        description = yaml.load(config_fp)
    with open(os.path.join(test_dir, pr.branch, DESCRIPTION_FILE_NAME)) as description_fp:
        md = description_fp.read()

    description['description'] = md
    description['id'] = pr.branch
    description['uploadDate'] = time.time()
    description['numSamples'] = pr.num_samples
    description['numFeatures'] = pr.feature_variables

    with open(out_file, 'w') as out_fp:
        json.dump(description, out_fp)

def send_report(pr):
    #pr.send_report(WISHBUILDER_EMAIL, WISHBUILDER_PASS, send_to='hillkimball@gmail.com')
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
