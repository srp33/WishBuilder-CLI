from SqliteDao import SqliteDao
from GithubDao import GithubDao
from WishBuilder import GH_TOKEN, test
import argparse

parser = argparse.ArgumentParser(description="Test pull requests on WishBuilder Repository")
group = parser.add_mutually_exclusive_group()
# group.add_argument("-v", "--verbose", action="store_true")
# group.add_argument("-q", "--quiet", action="store_true")
parser.add_argument("prID", type=int, help="Number of pull request")
args = parser.parse_args()
prID = args.prID

dao = SqliteDao('./history.sql')
dao.remove_pr(prID)
github = GithubDao('https://api.github.com/repos/srp33/WishBuilder/', GH_TOKEN)
pr = github.get_pr(prID)
test(pr, dao)

