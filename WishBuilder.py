from tests import *
from GithubDao import GithubDao
from SqliteDao import SqliteDao
from private import GH_TOKEN
from compare import *
from ShapeShifter import ShapeShifter
import shutil
import time
import json
import pickle
from multiprocessing import Process

sql_dao = None
git_dao = None


def check_history():
    history = sql_dao.get_all()
    prs = git_dao.get_prs()

    for pr in prs:
        if pr.pr in history.keys():
            if pr.sha not in history[pr.pr] and not sql_dao.in_progress(pr):
                return pr
        else:
            return pr
    return None


def get_new_prs():
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


def test(pr: PullRequest):
    cwd = os.getcwd()
    try:
        print("Testing {}, Pull Request #{}...".format(pr.branch, pr.pr), flush=True)
        start = time.time()
        raw_data_storage = os.path.join(RAW_DATA_STORAGE, pr.branch)
        if pr.branch not in os.listdir(TESTING_LOCATION):
            os.mkdir(os.path.join(TESTING_LOCATION, pr.branch))
        else:
            raise EnvironmentError("Directory {} Already Exists".format(os.path.join(TESTING_LOCATION, pr.branch)))
        pr.status = 'In progress'
        pr.email = git_dao.get_email(pr.sha)
        sql_dao.update(pr)
        files, download_urls = git_dao.get_files_changed(pr)
        valid, description_only = check_files_changed(pr, files)
        valid = True
        if valid:
            pr.report.valid_files = True
            if description_only:
                git_dao.merge(pr)
                geney_convert(pr)
                pr.set_updated()
            else:
                # Download Files from Github and put them in the testing directory
                download_urls.extend(git_dao.get_existing_files(pr.branch, files))
                download_urls.extend(git_dao.get_existing_files('Helper', files))
                for file in download_urls:
                    git_dao.download_file(file, TESTING_LOCATION)
                os.chdir(os.path.join(TESTING_LOCATION, pr.branch))
                # Run tests
                test_folder(pr)
                test_config(pr)
                test_files(pr)
                original_directory = os.listdir(os.getcwd())
                # original_directory.append('test_Clinical.tsv')
                # if this test doesn't pass, it is pointless to move on, because the output files will be wrong
                if test_scripts(pr):

                    # fix_files()

                    passed = check_test_for_every_data(pr, os.listdir(os.getcwd()))
                    if passed:
                        os.mkdir(raw_data_storage)
                        for file in os.listdir(os.getcwd()):
                            if file.endswith('.gz'):
                                os.system('mv {} {}'.format(file, raw_data_storage))
                    test_cleanup(original_directory, pr)
        pr.time_elapsed = time.strftime("%Hh:%Mm:%Ss", time.gmtime(time.time() - start))
        pr.date = time.strftime("%D", time.gmtime(time.time()))
        pr.e_date = time.time()
        pr.check_if_passed()
        sql_dao.update(pr)
        if pr.passed:
            convert_parquet(pr, raw_data_storage)
    except Exception as e:
        pr.status = 'Error'
        pr.passed = False
        pr.report.other = True
        pr.report.other_content = '\n### WishBuilder Error, we are working on it and will rerun your request when we' \
                                  ' fix the issue. (Error message: {})\n\n'.format(e)
    os.chdir(cwd)
    cleanup(pr)


def fix_files():
    files = os.listdir('./')
    if 'test_metadata.tsv' in files:
        shutil.move('test_metadata.tsv', 'test_Clinical.tsv')


def convert_parquet(pr: PullRequest, raw_data_storage):
    cwd = os.getcwd()
    os.chdir(raw_data_storage)
    geney_dataset_path = os.path.join(GENEY_DATA_LOCATION, pr.branch)
    os.mkdir(geney_dataset_path)
    data_files = os.listdir('./')
    groups = {}
    for file in data_files:
        group_name = file.rstrip('.gz').rstrip('.tsv')
        with gzip.open(file) as fp:
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
        os.remove(file)
        shutil.move('tmp.tsv.gz', file)
    num_features = 0
    for group in groups:
        num_features += len(groups[group])
    pr.feature_variables = num_features
    with open(os.path.join(geney_dataset_path, 'groups.json'), 'w') as fp_groups:
        json.dump(groups, fp_groups)
    data_path = os.path.join(geney_dataset_path, 'data.pq')
    ss = ShapeShifter(data_files[0])
    ss.merge_files(data_files[1:], data_path, 'parquet')
    get_metadata(data_path, os.path.join(geney_dataset_path, 'metadata.pkl'))
    get_description(pr, os.path.join(geney_dataset_path, 'description.json'))
    git_dao.merge(pr)
    os.chdir(cwd)


def get_metadata(data_file, out_file):
    ss = ShapeShifter(data_file)
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
    pr.send_report(recipient='hillkimball@gmail.com')
    try:
        pr.send_report()
    except Exception as e:
        print(e)
    print("Done!")


def simulate_test(pr: PullRequest):
    print("Starting job: {}".format(pr.branch), flush=True)
    time.sleep(20)
    print("testing {}...".format(pr.branch), flush=True)
    time.sleep(20)
    print("finished {}".format(pr.branch), flush=True)


def setup():
    os.chdir(WB_DIRECTORY)
    required_directories = [RAW_DATA_STORAGE, GENEY_DATA_LOCATION, TESTING_LOCATION]
    if not os.path.exists(PRIVATE_DATA):
        raise Exception("private.py does not exist")
    for path in required_directories:
        if not os.path.exists(path):
            os.makedirs(path)


if __name__ == '__main__':
    print(os.getcwd())
    setup()
    sql_dao = SqliteDao(SQLITE_FILE)
    git_dao = GithubDao('https://api.github.com/repos/srp33/WishBuilder/', GH_TOKEN)
    processes = []
    queue = []
    history = []
    while True:
        print("Check for prs", flush=True)
        new_prs = get_new_prs()
        for pull in new_prs:
            if pull.sha not in history:
                queue.append(pull)
        while len(queue) > 0:
            for p in processes:
                if not p.is_alive():
                    processes.remove(p)
            if len(processes) < MAX_NUM_PROCESSES:
                new_pr = queue.pop()
                history.append(new_pr.sha)
                p = Process(target=test, args=(new_pr,))
                processes.append(p)
                p.start()
            time.sleep(5)
        time.sleep(600)

    # new_prs = get_new_prs()
    # pr = new_prs[0]
    # print(pr.branch)
    # test(pr)
