import glob, json, logging, os, pickle, psutil, shutil, sys, time, inspect
from compare import *
from multiprocessing import Process
from GithubDao import GithubDao
from SqliteDao import SqliteDao
#sys.path.insert(0, '/ShapeShifter')
#import shapeshifter
from tests import *
from Constants import *
from Shared import *
from capturer import CaptureOutput
from FastFileHelper import *
import msgpack
#import ColumnInfo

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
                    shutil.rmtree(raw_data_storage, ignore_errors=True)
                    os.mkdir(raw_data_storage)

                    for f in tsv_file_paths:
                        os.system('mv {} {}/'.format(f, raw_data_storage))
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

    prefixes = []
    tsv_map_dirs = []

    for tsv_file in tsv_files:
        prefix = tsv_file.replace(".tsv", "")
        prefixes.append(prefix)
        tsv_map_dir = "{}.mp".format(prefix)

        shutil.rmtree(tsv_map_dir, ignore_errors=True)
        printToLog("Creating fast-file map for {}".format(tsv_file), pr)
        map_tsv(tsv_file, tsv_map_dir)
        printToLog("Done creating fast-file map for {}".format(tsv_file), pr)
        tsv_map_dirs.append(tsv_map_dir)

    merged_file = os.path.join(geney_dataset_path, "data.tsv")
    merged_map_dir = os.path.join(geney_dataset_path, "data.mp")

    if len(tsv_files) == 1:
        os.system("mv {} {}".format(tsv_files[0], merged_file))
        os.system("mv {} {}".format(tsv_map_dirs[0], merged_map_dir))

        features = [x.decode() for x in open_msgpack(os.path.join(merged_map_dir, 'features.msgpack'), 'rb')]
        feature_dict = {tsv_map_dirs[0]: features}
        num_features = len(features)
    else:
        printToLog("Creating merged file {} from {}".format(merged_file, " and ".join(tsv_files)), pr)
        feature_dict, num_features = merge_tsv(tsv_files, tsv_map_dirs, prefixes, merged_file, 50000)
        printToLog("Done creating merged file {}".format(merged_file), pr)

        printToLog("Creating fast-file map for {}".format(merged_file), pr)
        map_tsv(merged_file, merged_map_dir)
        printToLog("Done creating fast-file map for {}".format(merged_file), pr)

    pr.feature_variables = num_features

    printToLog("Creating JSON file", pr)
    with open(os.path.join(geney_dataset_path, 'groups.json'), 'w') as fp_groups:
        # Strip the .mp off the end of each group name
        feature_dict2 = {}
        for key, value in feature_dict.items():
            feature_dict2[key[:-3]] = feature_dict[key]

        json.dump(feature_dict2, fp_groups)
    printToLog("Done creating JSON file", pr)

    merged_transposed_file = os.path.join(geney_dataset_path, "transposed.tsv")
    merged_transposed_temp_dir = os.path.join(geney_dataset_path, "transposed.temp")
    merged_transposed_map_dir = os.path.join(geney_dataset_path, "transposed.mp")

    printToLog("Creating transposed file {}".format(merged_transposed_file), pr)
    transpose_tsv(merged_file, merged_map_dir, merged_transposed_file, merged_transposed_temp_dir, False, False, 100000000)
    printToLog("Done creating transposed file {}".format(merged_transposed_file), pr)

    printToLog("Creating fast-file map for {}".format(merged_transposed_file), pr)
    map_tsv(merged_transposed_file, merged_transposed_map_dir)
    printToLog("Done creating fast-file map for {}".format(merged_transposed_file), pr)

    printToLog("Saving metadata and description for {}".format(merged_transposed_file), pr)
    save_metadata(pr, merged_transposed_file, merged_transposed_map_dir, os.path.join(geney_dataset_path, 'metadata.pkl'))
    save_description(pr, test_dir, os.path.join(geney_dataset_path, DESCRIPTION_FILE_NAME))
    printToLog("Done saving metadata and description for {}".format(merged_transposed_file), pr)

    os.chdir(cwd)

    printToLog("Setting permissions on {}".format(geney_dataset_path), pr)
    os.system("chmod 777 {} -R".format(geney_dataset_path))

    return True

def save_metadata(pr: PullRequest, transposed_data_file, transposed_map_dir, out_file):
    printToLog("Saving metadata", pr)
    # In the transposed file, samples are actually features
    with open(os.path.join(transposed_map_dir, 'samples.msgpack'), 'rb') as samples_file:
        features = msgpack.unpack(samples_file)

    # Open the transposed data file so we can read feature values
    with open(os.path.join(transposed_map_dir, 'sample_data.msgpack'), 'rb') as map_file:
        data_map = msgpack.unpack(map_file)

    meta_dict = {}

    with open(transposed_data_file) as transposed_file:
        for i in range(len(features)):
            if i > 0 and i % 1000 == 0:
                printToLog("{}".format(i), pr)

            feature = features[i]
            feature_coordinates = data_map[feature]
            transposed_file.seek(feature_coordinates[0])

            feature_values = [x for x in transposed_file.read(feature_coordinates[1]).split("\t") if x != "NA"]
            feature_values = sorted(list(set(feature_values)))

            # Check whether we only had missing (NA) values
            if len(feature_values) == 0:
                meta_dict[feature] = {'options': ["NA"], 'numOptions': 1}
            else:
                float_values = convert_to_floats(feature_values)

                if not float_values:
                    meta_dict[feature] = {'options': feature_values, 'numOptions': len(feature_values)}
                else:
                    meta_dict[feature] = {'options': 'continuous', 'min': min(float_values), 'max': max(float_values)}

    metadata = {'meta': meta_dict}
    with open(out_file, 'wb') as fp:
        pickle.dump(metadata, fp)

def convert_to_floats(str_list):
    try:
        return [float(x) for x in str_list]
    except:
        return False

def save_description(pr: PullRequest, test_dir, out_file):
    printToLog("Saving description", pr)

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
