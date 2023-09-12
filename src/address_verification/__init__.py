import configparser
import logging

from celery import Celery
from kombu import Queue, Exchange
from geopy.geocoders import Nominatim

# setup
LOGGER = logging.getLogger(__name__)
cparser = configparser.ConfigParser() # objects to make available when this package is imported
cparser.read('conf/pipeline.conf')

## configuration: read from configparser - https://docs.celeryq.dev/en/stable/userguide/configuration.html
class CeleryConfig:
    broker_url =  "pyamqp://guest@localhost//"
    result_backend = "db+postgresql://celery_user:celery_pass@localhost/celery_db"
    include = ["address_verification.tasks"]
    event_queue_expires = 3600
    worker_hijack_root_logger = False
    task_queues = (
            Queue("default", Exchange("default"), routing_key="default"),
            Queue("address_verification_queue", Exchange("address_verification_queue"), routing_key="ctq")
    )

# start application
app = Celery('tasks')
LOGGER.info(f'celery-instance:{app} - celery app instantiated')

# set app configuration
app.config_from_object(CeleryConfig)

# geolocator instance
geolocator = Nominatim(user_agent="address_verification")
