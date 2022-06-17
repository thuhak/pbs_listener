import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import logging
from threading import Lock

from jinja2 import Environment, PackageLoader

logger = logging.getLogger(__name__)
env = Environment(loader=PackageLoader('extensions', 'mail_templates'))


class Mail:
    def __init__(self, smtp_host: str, smtp_user: str, smtp_pass: str, smtp_port=25):
        self.server = smtplib.SMTP()
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_pass = smtp_pass
        self.template = env.get_template('job_finish')
        self.export_keys = ['job_name', 'job_id', 'total_hours', 'wait_hours', 'run_hours', 'app', 'job_file', 'cores', 'memory']
        self.lock = Lock()

    def _is_connected(self):
        try:
            status = self.server.noop()[0]
        except:
            status = -1
        return True if status == 250 else False

    def __call__(self, jobdata):
        email = jobdata.get('email')
        if not email:
            logger.error('no email info')
            return
        job_ret = 'completed' if jobdata['is_job_success'] else 'failed'
        msg = MIMEMultipart()
        msg['From'] = Header(self.smtp_user, 'ascii')
        msg['To'] = Header(email, 'ascii')
        msg['Subject'] = Header(f'Your HPC job is {job_ret}')
        email_data = jobdata.export(self.export_keys)
        logger.debug(f'email data: {email_data}')
        content = self.template.render(**email_data)
        msg.attach(MIMEText(content, 'plain', 'utf-8'))
        with self.lock:
            logger.info(f'sending mail to {email}')
            try:
                if not self._is_connected():
                    self.server.connect(self.smtp_host, self.smtp_port)
                    self.server.login(self.smtp_user, self.smtp_pass)
                self.server.sendmail(self.smtp_user, [email], msg.as_string())
            except Exception as e:
                logger.error(f'send mail failed, reason {str(e)}')
            finally:
                self.server.close()