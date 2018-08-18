import os

MAX_NUM_PROCESSES = int(os.environ["MAX_NUM_PROCESSES"])
REPO_URL = 'https://api.github.com/repos/srp33/WishBuilder/'
WB_DIRECTORY = "/Shared"
SQLITE_FILE = os.path.join(WB_DIRECTORY, 'history.sql')
PRS_TO_DELETE_FILE = os.path.join(WB_DIRECTORY, 'prs_to_delete.txt')
TESTING_LOCATION = os.path.join(WB_DIRECTORY, 'testing/')
RAW_DATA_STORAGE = os.path.join(WB_DIRECTORY, 'RawDatasets/')
GENEY_DATA_LOCATION = os.path.join(WB_DIRECTORY, 'GeneyDatasets/')
MIN_TEST_CASES = 8
MIN_FEATURES = 2
MIN_SAMPLES = 2
MAX_TITLE_SIZE = 300
NUM_SAMPLE_ROWS = 5
NUM_SAMPLE_COLUMNS = 5
CHECK_MARK = '&#9989;'
RED_X = '&#10060;'
WARNING_SYMBOL = "<p><font color=\"orange\" size=\"+2\">&#9888;\t</font>"
DOWNLOAD_FILE_NAME = 'download.sh'
INSTALL_FILE_NAME = 'install.sh'
PARSE_FILE_NAME = 'parse.sh'
CLEANUP_FILE_NAME = 'cleanup.sh'
DESCRIPTION_FILE_NAME = 'description.md'
CONFIG_FILE_NAME = 'config.yaml'
REQUIRED_FILES = [DOWNLOAD_FILE_NAME, INSTALL_FILE_NAME, PARSE_FILE_NAME,
                  CLEANUP_FILE_NAME, DESCRIPTION_FILE_NAME, CONFIG_FILE_NAME]
REQUIRED_CONFIGS = ['title', 'featureDescription', 'featureDescriptionPlural']
# These are the executables that will be ran to produce the data and metadata files (They are executed in this order)
USER_SCRIPTS = [INSTALL_FILE_NAME, DOWNLOAD_FILE_NAME, PARSE_FILE_NAME]
