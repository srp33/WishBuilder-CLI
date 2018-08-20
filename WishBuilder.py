import json, logging, os, pickle, psutil, shutil, sys, time, inspect
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

#class StreamToLogger(object):
#    def __init__(self, logger, log_level=logging.INFO):
#        self.logger = logger
#        self.log_level = log_level
#        self.linebuf = ''
#
#    def write(self, buf):
#        for line in buf.rstrip().splitlines():
#            self.logger.log(self.log_level, line.rstrip())
#
#    def flush(self):
#        pass

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
        error += ' File "{1}", line {2}, in {3}\n<br><br>\n'.format(*item)
        for line in item[4]:
            error += ' ' + line.lstrip()
        for item in inspect.getinnerframes(tb):
            error += ' File "{1}", line {2}, in {3}\n<br><br>\n'.format(*item)
        for line in item[4]:
            error += ' ' + line.lstrip()

    error += "<br>\n<br><b>" + str(e) + "</b>"

    return error

def test(pr: PullRequest, sql_dao):
    cwd = os.getcwd()
    try:
        shutil.rmtree(os.path.join(TESTING_LOCATION, pr.branch), ignore_errors=True)
        os.mkdir(os.path.join(TESTING_LOCATION, pr.branch))

#        logging.basicConfig(
#            level=logging.DEBUG,
#            #format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
#            format='%(asctime)s - %(message)s',
#            filename=log_file_path,
#            filemode='a')
#
#        thread_logger = logging.getLogger(new_pr.branch)
#        sys.stdout = StreamToLogger(thread_logger, logging.INFO)
#        sys.stderr = StreamToLogger(thread_logger, logging.ERROR)
#        thread_logger.info("Starting logger for " + new_pr.branch + "...")

        printToLog("Testing {}, Pull Request #{}...".format(pr.branch, pr.pr), pr)

        pr.status = 'In progress'
        pr.email = git_dao.get_email(pr.sha)
        pr.log_file_path = os.path.join(os.path.join(TESTING_LOCATION, pr.branch), LOG_FILE_NAME)
        sql_dao.update(pr)

        cleanup(pr)
        start = time.time()
        raw_data_storage = os.path.join(RAW_DATA_STORAGE, pr.branch)

        files, download_urls, removed_files = git_dao.get_files_changed(pr)
        valid, description_only = check_files_changed(pr, files)

        valid = True
        if valid:
            pr.report.valid_files = True
            if description_only:
                convert_parquet(pr, raw_data_storage)
                git_dao.merge(pr)
                pr.set_updated()
            else:
                # Download Files from Github and put them in the testing directory
                download_urls.extend(git_dao.get_existing_files(pr.branch, files, removed_files))
                download_urls.extend(git_dao.get_existing_files('Helper', files, []))

                for f in download_urls:
                    git_dao.download_file(f, TESTING_LOCATION)

                os.chdir(os.path.join(TESTING_LOCATION, pr.branch))

                # Run tests
                test_folder(pr)
                test_config(pr)
                test_files(pr)

                original_directory = os.listdir(os.getcwd())

                # if this test doesn't pass, it is pointless to move on, because the output files will be wrong
                if test_scripts(pr):
                    fix_files()

                    passed = check_test_for_every_data(pr, os.listdir(os.getcwd()))

                    if passed:
                        shutil.rmtree(raw_data_storage, ignore_errors=True)
                        os.mkdir(raw_data_storage)

                        for f in os.listdir(os.getcwd()):
                            if f.endswith('.gz'):
                                os.system('mv {} {}'.format(f, raw_data_storage))

                    test_cleanup(original_directory, pr)

        pr.time_elapsed = time.strftime("%Hh:%Mm:%Ss", time.gmtime(time.time() - start))
        pr.date = time.strftime("%D", time.gmtime(time.time()))
        pr.e_date = time.time()
        pr.check_if_passed()
        sql_dao.update(pr)

        if pr.passed:
            convert_parquet(pr, raw_data_storage)
            git_dao.merge(pr)
    except Exception as e:
        pr.status = 'Error'
        pr.passed = False
        pr.report.other = True
        #pr.report.other_content = '\n### WishBuilder Error, we are working on it and will rerun your request when we fix the issue. (Error message: {})\n\n'.format(e)
        pr.report.other_content = get_exception_stack(e)

    os.chdir(cwd)
    send_report(pr)
    cleanup(pr)

def fix_files():
    files = os.listdir('./')
    if 'test_metadata.tsv' in files:
        shutil.move('test_metadata.tsv', 'test_Clinical.tsv')

def convert_parquet(pr: PullRequest, raw_data_storage):
    printToLog("Building parquet file(s)...", pr)

    cwd = os.getcwd()
    os.chdir(raw_data_storage)

    geney_dataset_path = os.path.join(GENEY_DATA_LOCATION, pr.branch)
    shutil.rmtree(geney_dataset_path, ignore_errors=True)
    os.mkdir(geney_dataset_path)
    data_files = os.listdir('./')

    groups = {}
    for f in data_files:
        group_name = f.rstrip('.gz').rstrip('.tsv')
        with gzip.open(f) as fp:
            with gzip.open('tmp.tsv.gz', 'w') as fp_out:
                columns = fp.readline().decode().rstrip('\n').split('\t')
                groups[group_name] = [columns[0]]
                for column in columns[1:]:
                    option = '{}_{}'.format(group_name, column)
                    groups[group_name].append(option)
                fp_out.write('\t'.join(groups[group_name]).encode())
                fp_out.write('\n'.encode())
                for line in fp:
                    fp_out.write(line)
                groups[group_name].remove(columns[0])
        os.remove(f)
        shutil.move('tmp.tsv.gz', f)

    num_features = 0
    for group in groups:
        num_features += len(groups[group])

    pr.feature_variables = num_features
    with open(os.path.join(geney_dataset_path, 'groups.json'), 'w') as fp_groups:
        json.dump(groups, fp_groups)

    data_path = os.path.join(geney_dataset_path, 'data.pq')
    ss = ShapeShifter.ShapeShifter(data_files[0])
    ss.merge_files(data_files[1:], data_path, 'parquet')
    get_metadata(data_path, os.path.join(geney_dataset_path, 'metadata.pkl'))
    get_description(pr, os.path.join(geney_dataset_path, 'description.json'))
    os.chdir(cwd)

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

def get_description(pr: PullRequest, out_file):
    with open(os.path.join(TESTING_LOCATION, pr.branch, 'config.yaml')) as config_fp:
        description = yaml.load(config_fp)
    with open(os.path.join(TESTING_LOCATION, pr.branch, 'description.md')) as description_fp:
        md = description_fp.read()

    description['description'] = md
    description['id'] = pr.branch
    description['uploadDate'] = time.time()
    description['numSamples'] = pr.num_samples
    description['numFeatures'] = pr.feature_variables

    with open(out_file, 'w') as out_fp:
        json.dump(description, out_fp)

def cleanup(pr):
    shutil.rmtree("{}".format(os.path.join(TESTING_LOCATION, pr.branch)), ignore_errors=True)

def send_report(pr):
    #pr.send_report(WISHBUILDER_EMAIL, WISHBUILDER_PASS, send_to='hillkimball@gmail.com')
    pr.send_report(WISHBUILDER_EMAIL, WISHBUILDER_PASS, send_to='stephen.piccolo.byu@gmail.com')

    try:
        pr.send_report(WISHBUILDER_EMAIL, WISHBUILDER_PASS)
    except Exception as e:
        printToLog(get_exception_stack(e), pr)

    printToLog("Sent email report", pr)

def setup():
    os.chdir(WB_DIRECTORY)
    required_directories = [RAW_DATA_STORAGE, GENEY_DATA_LOCATION, TESTING_LOCATION]

    for path in required_directories:
        if not os.path.exists(path):
            os.makedirs(path)

if __name__ == '__main__':
    with CaptureOutput() as capturer:
        GH_TOKEN = os.environ['GH_TOKEN']
        WISHBUILDER_EMAIL = os.environ['WISHBUILDER_EMAIL']
        WISHBUILDER_PASS = os.environ['WISHBUILDER_PASS']
        SLEEP_SECONDS = int(os.environ['SLEEP_SECONDS'])

        setup()
        sql_dao = SqliteDao(SQLITE_FILE)
        git_dao = GithubDao('https://api.github.com/repos/srp33/WishBuilder/', GH_TOKEN)

        if os.path.exists(PRS_TO_DELETE_FILE):
            with open(PRS_TO_DELETE_FILE) as prFile:
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
