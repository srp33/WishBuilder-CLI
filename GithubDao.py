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

    def get_files_changed(self, pr: PullRequest):
        url = self.repo_url + 'pulls/{}/files'.format(pr.pr)
        payload = requests.get(url, headers=self.head).json()

        changed_files = []

        for i in range(len(payload)):
            file_name = payload[i]['filename']
            status = payload[i]['status']

            if status != "removed":
                changed_files.append(file_name)

        return changed_files

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

    def get_branch(self, pr, destDir):
        cloneUrl = "https://github.com/{}/WishBuilder.git".format(pr.repo_owner)

        pwd = os.getcwd()
        tmpDir = tempfile.mkdtemp()
        os.chdir(tmpDir)

        cloneCommand = "git clone {}; cd WishBuilder; git config user.name 'Snail Mail'; git config user.email '<>'; git pull origin {}".format(cloneUrl, pr.branch)
        execShellCommand(cloneCommand, pr)

        mvCommand = "mv WishBuilder/Helper {}/; mv WishBuilder/{} {}/".format(destDir, pr.branch, destDir)
        execShellCommand(mvCommand, pr)

        os.chdir(pwd)
        shutil.rmtree(tmpDir, ignore_errors=True)
