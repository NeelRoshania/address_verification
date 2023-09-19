import json
import logging
import time

from address_verification import app, geolocator
from address_verification.funcs import get_duration, exponential_backoff
from address_verification.psql import connect_postgres
from celery.app.log import TaskFormatter
from celery.utils.log import get_task_logger
from celery.result import AsyncResult
from celery.signals import task_success, task_retry, after_setup_task_logger
from kombu.exceptions import OperationalError
from multiprocessing.dummy import Pool
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderRateLimited
from geopy.location import Location

"""
    References
        - task requests, https://docs.celeryq.dev/en/stable/userguide/tasks.html#task-request
        - signals,  https://docs.celeryq.dev/en/stable/userguide/signals.html#signal-ref
        - chain tasks together using thier signatures - https://docs.celeryq.dev/en/stable/userguide/tasks.html#task-synchronous-subtasks
        
        - memoryview's (to deserialize data objects, and provide a method of exposing underlying data without copying)
            Certain objects available in Python wrap access to an underlying memory array or buffer. 
            Such objects include the built-in bytes and bytearray, and some extension types like array.array. 
            Third-party libraries may define their own types for special purposes, such as image processing or numeric analysis.

            While each of these types have their own semantics, they share the common characteristic of being backed by a possibly large memory buffer. 
            It is then desirable, in some situations, to access that buffer directly and without intermediate copying.

            - https://docs.python.org/3/library/stdtypes.html#memoryview
            - https://docs.python.org/3/c-api/buffer.html#bufferobjects

            - defining memoryviews from arrays
                - https://docs.python.org/3/library/stdtypes.html#memoryview.tolist
                - https://docs.python.org/3/library/array.html

"""
# logging configurations
LOGGER = get_task_logger(__name__) # this should call the logger celery_template.tasks

# signals

# define task logger and redirect to file
@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    LOGGER = logging.getLogger(__name__)
    LOGGER.handlers.clear()
    LOGGER.addHandler(logging.FileHandler(f'logs/{__name__}.log'))
    for handler in LOGGER.handlers:
        handler.setFormatter(TaskFormatter('[%(asctime)s:%(task_id)s:%(task_name)s:%(name)s:%(levelname)s] %(message)s'))
    return None

# handle task successes
@task_success.connect
def log_task_id(sender=None, result=None, **kwargs) -> tuple:
    print(f'{LOGGER.name}, handlers: {LOGGER.handlers}')
    LOGGER.info(f'task success - sender:{type(sender)} completed with result: {type(result)}')
    return None

# signal to handle task successes
@task_retry.connect
def retry_feedback(sender=None, request=None, reason=None, einfo=None, **kwargs) -> tuple:
    LOGGER.info(f'task retrying - {reason})')
    return None

# tasks

@app.task(bind=True)
def geopy_verify_address(self, address: str) -> dict:

    """
        A geopy address verification task that can fail due to a timeout

    """
    start_time = time.time()

    # LOGGER.info(f'task.request:{dir(self.request)} - args=({x}, {y})')

    try:
        
        res = geolocator.geocode(address)
        end_time = time.time()
        return {
            "task_description": 'geopy_verify_address',
            "completed": True,
            "duration": get_duration(start_time=start_time, end_time=end_time),
            "result": (res.raw['place_id'], res.raw['osm_id'], res.raw['osm_type'], res.address) if isinstance(res, Location) else None
        }
        
    except (GeocoderTimedOut, GeocoderUnavailable, GeocoderRateLimited) as e:
        
        LOGGER.error(f'task failed - geopy service timed out')

        # If retried, will run the task with the intially supplied arguments unless..
        raise self.retry(
            countdown=exponential_backoff(5, self.request.retries), # custom back-off
            max_retries=3,
            # retry_backoff=30,
            # retry_jitter=False,
            # retry_backoff_max=6*50,
            exc=e
        )

# non-tasks

def fetch_task_result(taskid: str) -> tuple:
    _res = AsyncResult(id=taskid, app=app)
    if _res.state in ['SUCCESS', 'FAILURE']:
        return taskid, _res.state, _res.date_done.isoformat(), _res.result
    else:
        return taskid, _res.state,

def fetch_task_results(task_ids: list[str]) -> list:

    """
        Gather task results asynchronously
            - Tasks that take time to complete should be waited and returned when complete

    """

    with Pool() as pool:
        results = pool.map(fetch_task_result, task_ids)

    return results

def await_tasks_completion(taskids: list) -> None:
    
    """
        Continuously check task status and terminate when there are not more tasks being retried

    """
    LOGGER.info(f'checking status of tasks: {len(taskids)}')
    
    while True:
        res = fetch_task_results(taskids) 
        if len([r[0] for r in res if r[1] in ('RETRY', 'PENDING')]) == 0:
            break

    LOGGER.info(f'all tasks complete')
    return res
