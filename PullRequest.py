import os, sys
from Report import Report
from datetime import datetime, timedelta
import markdown
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

class PullRequest:
    def __init__(self, pr: int, branch: str, repo_owner: str, date: str, e_date: float, feature_variables: int, meta_variables: int,
                 passed: bool, pr_id: int, num_samples: int, sha: str, time_elapsed: str, user: str, email: str,
                 status: str, report: str = None):
        self.pr = pr
        self.branch = branch
        self.repo_owner = repo_owner
        self.date = date
        self.e_date = e_date
        self.feature_variables = feature_variables
        self.meta_variables = meta_variables
        self.passed = passed
        self.pr_id = pr_id
        self.num_samples = num_samples
        self.sha = sha
        self.time_elapsed = time_elapsed
        self.user = user
        self.email = email
        self.status = status
        self.report = Report(report)
        self.log_file_path = None

    def __str__(self) -> str:
        out = "Pull Request Number: #{}\nBranch: {}\nRepo Owner: {}\nDate: {}\neDate: {}\nNumber of Feature Variables: {}\n" \
              "Number of Metadata Variables: {}\nPassed: {}\nPull Request ID: {}\nNumber of Samples: {}\nSha: " \
              "{}\nTime Elapsed: {}\nUser: {}\nEmail: {}\nStatus: {}" \
            .format(self.pr, self.branch, self.repo_owner, self.date, self.e_date, self.feature_variables, self.meta_variables,
                    self.passed, self.pr_id, self.num_samples, self.sha, self.time_elapsed, self.user, self.email,
                    self.status)
        return out

    def set_updated(self):
        self.status = 'Updated'
        self.passed = True
        self.date = (datetime.now() - timedelta(hours=7)).strftime("%b %d, %y. %H:%m MST")

    def get_report_markdown(self) -> str:
        out = "<h1><center>{}</center></h1>\n".format(self.branch)
        out += '<h2><center> Status: {} </center></h2>\n<center>{}</center>\n\n'.format(self.status, self.date)
        out += str(self.report)
        return out

    def get_report_html(self) -> str:
        md = self.get_report_markdown()
        html = markdown.markdown(md)
        return html

    # From https://stackoverflow.com/questions/3362600/how-to-send-email-attachments
    def send_report(self, username, password, send_to='user'):
        if send_to == 'user':
            send_to = self.email

        if self.passed:
            subject = "Passed: {}".format(self.branch)
        else:
            subject = "Failed: {}".format(self.branch)

        send_from = 'wishbuilder@kimball-hill.com'

        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = send_to
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject

        msg.attach(MIMEText(self.get_report_html(), 'html'))

        if os.path.exists(self.log_file_path):
            part = MIMEBase('application', "octet-stream")
            with open(self.log_file_path, 'rb') as file:
                part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="{}"'.format(os.path.basename(self.log_file_path)))
            msg.attach(part)

        smtp = smtplib.SMTP("mail.kimball-hill.com", 587)
        smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.quit()

    def check_if_passed(self) -> bool:
        passed = True
        if not self.report.valid_files:
            passed = False
        if not self.report.pass_directory_test:
            passed = False
        if not self.report.pass_configuration_test:
            passed = False
        if not self.report.pass_file_test:
            passed = False
        if not self.report.pass_gzip_test:
            passed = False
        if not self.report.pass_script_test:
            passed = False
        if not self.report.pass_key_test:
            passed = False
        if not self.report.pass_data_tests:
            passed = False

        self.passed = passed

        if passed:
            self.status = 'Complete'
            return True
        else:
            self.status = 'Failed'
            return False
