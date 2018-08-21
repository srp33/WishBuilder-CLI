import base64, os, requests, shutil, sys, tempfile
from PullRequest import PullRequest
from Constants import REPO_URL, TESTING_LOCATION
from Shared import *

class GithubDao:
    def __init__(self, repo_url: str, token: str):
        if repo_url[-1] != '/':
            repo_url += '/'
        self.repo_url = repo_url
        self.token = token
        if len(token) > 0:
            self.head = {'Authorization': 'token {}'.format(token)}
        else:
            self.head = None

    def get_prs(self):
        url = self.repo_url + 'pulls'
        payload = requests.get(url, headers=self.head).json()

        if "message" in payload:
            print("Message from GitHub: " + payload["message"])
            sys.exit(1)

        prs = []
        if type(payload) == 'dict':
            return []

        for i in range(len(payload)):
            branch = payload[i]['head']['ref']
            repo_owner = payload[i]['head']['user']['login']
            pr_id = payload[i]['id']
            pr_num = payload[i]['number']
            user = payload[i]['user']['login']
            sha = payload[i]['head']['sha']
            pr = PullRequest(int(pr_num), branch, repo_owner, 'N/A', 0, 0, 0, False, int(pr_id), 0, sha, 'N/A', user, 'N/A', 'N/A')
            prs.append(pr)

        return prs

    def get_pr(self, pr_num):
        all_prs = self.get_prs()
        for pr in all_prs:
            if pr.pr == int(pr_num):
                return pr
        return None

#    def get_files_changed(self, pr: PullRequest):
#        url = self.repo_url + 'pulls/{}/files'.format(pr.pr)
#        #print(url)
#        payload = requests.get(url, headers=self.head).json()
#
#        status_dict = {}
#        #url_dict = {}
#        #renamed = set()
#
#        for i in range(len(payload)):
#            file_name = payload[i]['filename']
#            url = payload[i]['raw_url']
#            status = payload[i]['status']
#
#            status_dict[file_name] = status
#            url_dict[file_name] = status
#
#            if status == "renamed":
#                renamed.add(payload[i]['previous_filename'])
#
#        return status_dict, url_dict, renamed

#    def check_files(self, pr: PullRequest):
#        url = self.repo_url + 'pulls/{}/files'.format(pr.pr)
#        payload = requests.get(url, headers=self.head).json()
#        files = []
#        bad_files = []
#        download_urls = []
#        for i in range(len(payload)):
#            files.append(payload[i]['filename'])
#            download_urls.append(payload[i]['raw_url'])
#        for fileName in files:
#            if fileName.split("/")[0] != pr.branch:
#                bad_files.append(fileName)
#        if len(bad_files) > 0:
#            pr.status = 'Failed'
#            report = "Only files in the \"{branch}\" directory should be changed. The following files were " \
#                     "also changed in this branch:\n".format(branch=pr.branch)
#            for file in bad_files:
#                report += "- {file}\n".format(file=file)
#            pr.report.valid_files = False
#            pr.report.valid_files_report = report
#            valid = False
#        else:
#            valid = True
#        for fileName in files:
#            if fileName != "{}/description.md".format(pr.branch) and fileName != "{}/config.yaml".format(pr.branch):
#                return valid, False, download_urls
#        return valid, True, download_urls

    def merge(self, pr: PullRequest) -> bool:
        request = {'sha': pr.sha}
        url = '{repo}pulls/{num}/merge?access_token={token}'.format(repo=self.repo_url, num=pr.pr, token=self.token)
        response = requests.put(url, json=request)
        if 'Pull Request successfully merged' in response.json()['message']:
            printToLog('Pull Request #{num}, Branch \"{branch}\", has been merged to WishBuilder Master branch'.format(num=pr.pr, branch=pr.branch), pr)
            return True
        else:
            printToLog('Pull Request #{num}, Branch \"{branch}\", could not be merged to WishBuilder Master branch'.format(num=pr.pr, branch=pr.branch), pr)
            printToLog(response, pr)
            printToLog(response.json()['message'], pr)
            return False

    def get_email(self, sha: str) -> str:
        url = '{}git/commits/{}'.format(self.repo_url, sha)
        response = requests.get(url, headers=self.head).json()
        #email = response['author']['email']
        email = "steve.piccolo@gmail.com"
        return email

#    def make_request(self, url: str, request_type: str='get', authorization: str=None, full_url: bool = False):
#        if not full_url:
#            url = self.repo_url + url
#        if request_type == 'get':
#            response = requests.get(url, headers=self.head).json()
#        else:
#            response = requests.put(url, headers=self.head).json()
#        return response

#    def get_contents(self, path):
#        if not path[0] == '/':
#            path = "/{}".format(path)
#
#        url = "{}contents{}".format(self.repo_url, path)
#        response = requests.get(url, headers=self.head).json()
#        return response
#
#    def get_helper_files(self, pr):
#        #response = self.get_contents(pr.branch + "/Helper")
#        response = self.get_contents("Helper")
#
#        if type(response) is dict:
#            if response['message']:
#                printToLog(response['message'])
#                sys.exit(1)
#
#        file_dict = {}
#
#        for i in range(len(response)):
#            file_path = response[i]['path']
#            file_type = response[i]['type']
#            download_url = response[i]['download_url']
#
#            if file_type == 'file':
#                file_dict[file_path] = download_url
#            elif file_type == 'dir':
#                file_dict.update(self.get_helper_files(file_path))
#
#        return file_dict

#    def get_existing_files(self, directory, files, removed_files):
#        response = self.get_contents(directory)
#
#        if type(response) is dict:
#            if response['message']:
#                return []
#
#        existing_files = []
#        for i in range(len(response)):
#            file_path = response[i]['path']
#
#            if file_path in removed_files:
#                continue
#
#            if file_path not in files:
#                if response[i]['type'] == 'dir':
#                    existing_files.extend(self.get_existing_files(file_path, files, removed_files))
#                elif response[i]['type'] == 'file':
#                    existing_files.append(response[i]['download_url'])
#
#        return existing_files

    def get_branch(self, pr, destDir):
        cloneUrl = "https://github.com/{}/WishBuilder.git".format(pr.repo_owner)

        pwd = os.getcwd()
        tmpDir = tempfile.mkdtemp()
        os.chdir(tmpDir)

        cloneCommand = "git clone {}; cd WishBuilder; git pull origin {}".format(cloneUrl, pr.branch)
        execShellCommand(cloneCommand)

        mvCommand = "mv WishBuilder/Helper {}/; mv WishBuilder/{} {}/".format(destDir, pr.branch, destDir)
        execShellCommand(mvCommand)

        os.chdir(pwd)
        shutil.rmtree(tmpDir, ignore_errors=True)

#    def get_branch_files(self, pr):
#        url = '{}git/commits/{}'.format(self.repo_url, pr.sha)
#        url = url.replace("/{}/".format(REPO_OWNER), "/{}/".format(pr.repo_owner))
#        response = requests.get(url, headers=self.head).json()
#
#        branchUrl = response['tree']['url']
#        response = requests.get(branchUrl, headers=self.head).json()
#
#        pathUrl = [pathDict['url'] for pathDict in response['tree'] if pathDict['path'] == pr.branch][0]
#        helperUrl = [pathDict['url'] for pathDict in response['tree'] if pathDict['path'] == "Helper"][0]
#        print(helperUrl)
#        print(response)
#        sys.exit(1)
#        response = requests.get(pathUrl, headers=self.head).json()
#
#        branchDict = {}
#        for pathDict in response['tree']:
#            filePath = os.path.join(pr.branch, pathDict['path'])
#
#            fileUrl = pathDict['url']
#            response = requests.get(fileUrl, headers=self.head).json()
#
#            fileContent = base64.b64decode(response['content'])
#            branchDict[filePath] = fileContent
#
#        return branchDict

#    def download_file(self, url: str, destination: str= './'):
#        split_url = url.split('/')
#        if 'raw' in split_url:
#            i = split_url.index('raw')
#        else:
#            i = split_url.index('master') - 1
#        local_path = destination + "/".join(split_url[i+2:-1])
#        local_filename = destination + "/".join(split_url[i+2:])
#        if not os.path.exists(local_path):
#            os.makedirs(local_path)
#        # local_filename = destination + url.split('/')[-1]
#        response = requests.get(url, stream=True, headers=self.head)
#        with open(local_filename, 'wb') as fs:
#            for chunk in response.iter_content(chunk_size=1024):
#                if chunk:
#                    fs.write(chunk)
#        return local_filename
