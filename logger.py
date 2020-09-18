from google.cloud import logging as gcplog
import logging

client = gcplog.Client()
client.setup_logging()

logging.info('')
logging.warning('testing')

