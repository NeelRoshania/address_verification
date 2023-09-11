import json
import logging
import time

from address_verification import app
from address_verification.funcs import get_duration, exponential_backoff
from address_verification.csv import read_csv
from address_verification.psql import connect_postgres
from celery.app.log import TaskFormatter
from celery.utils.log import get_task_logger
from celery.result import AsyncResult
from celery.signals import task_success, task_retry, after_setup_task_logger
from kombu.exceptions import OperationalError
from multiprocessing.dummy import Pool

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
def failed_task(self, prob: int) -> dict:

    """
        A task that fails with arbitrary retry logic

    """
    start_time = time.time()

    # LOGGER.info(f'task.request:{dir(self.request)} - args=({x}, {y})')

    try:

        if prob > 0.8:
            LOGGER.info(f'no fail')
            end_time = time.time()
            return {
                "task_description": 'failed_task',
                "completed": True,
                "duration": get_duration(start_time=start_time, end_time=end_time),
                "result": prob
            }
        else:
            raise ZeroDivisionError('divide by zero not alowed - float division by zero') # raise a know exception
    
    except ZeroDivisionError as e:
        
        LOGGER.error(f'task failed - handling known exception ZeroDivisionError')

        # If retried, will run the task with the intially supplied arguments unless..
        raise self.retry(
            countdown=exponential_backoff(2, self.request.retries), # custom back-off
            max_retries=3,
            # retry_backoff=30,
            # retry_jitter=False,
            # retry_backoff_max=6*50,
            exc=e
        )

@app.task(bind=True)
def sort_list(self, fpath: str) -> dict:

    """
        task is packaged to perform bubble_sort 

    """
    def bubble_sort(arr: list) -> list:
        
        """
            bubble sort implementation
                - https://www.programiz.com/dsa/bubble-sort
        """ 

        # loop to access each array element
        if isinstance(arr, list):

            for i in range(len(arr)):

                # loop to compare array elements
                for j in range(0, len(arr) - i - 1):

                    # compare two adjacent elements - change > to < to sort in descending order
                    if arr[j] > arr[j + 1]:

                        # swapping elements if elements are not in the intended order
                        temp = arr[j]
                        arr[j] = arr[j+1]
                        arr[j+1] = temp
            
            return arr
        else:
            raise TypeError(f'argument of type {type(arr)} must be {type([])}: {arr}')
    
    """
        Sort one list object 
    """

    start_time = time.time()
    lsorted = []
    headers, *data = read_csv(file_loc=fpath)
    LOGGER.info(f'sorting data in: {fpath}')

    for row in data:
        l = json.loads(row[1])
        lsorted.append([
                        row[0],
                        bubble_sort(l)
                    ]
        )
    end_time = time.time()

    # exposing data to an object in memory is not recommended
    return {
        "task_description": 'single-sort',
        "completed": True,
        "duration": get_duration(start_time=start_time, end_time=end_time),
        "result": lsorted
        # "result": memoryview(array.array('l', lsorted)), # celery can't pickle this
    }

# this is a bad practice - chain tasks together using thier signatures - https://docs.celeryq.dev/en/stable/userguide/tasks.html#task-synchronous-subtasks
@app.task(bind=True)
def sort_directory(self, fpaths: list) -> None:

    """
        Sort data across many files
            - arguably redudent, 
                - but required to showcase that tasks should call references to the most recent verison of data objects
                - in this case, a path to a file on disk

    """

    start_time = time.time()
    fsorted = []
    
    for path in enumerate(fpaths):
        fsorted.append([
            path[1],
            sort_list(fpath=path[1])["result"]
            # sort_list(fpath=path[1])
            ]
        )
    end_time = time.time()

    return {
        "task_description": 'sort-directory',
        "completed": True,
        "duration": get_duration(start_time=start_time, end_time=end_time),
        "result": fsorted
        # "result": memoryview(array.array('l', fsorted)), # celery cannot pickle this
    }

@app.task(bind=True)
def add(self, x, y):

    """
        A very simple task

    """
    start_time = time.time()
    LOGGER.info(f'task.request:{dir(self.request)} - args=({x}, {y})')
    end_time = time.time()
    return {
        "task_description": 'add',
        "completed": True,
        "duration": get_duration(start_time=start_time, end_time=end_time),
        "result": x+y
    }

# non-tasks

def fetch_task_result(taskid: str) -> tuple:
    _res = AsyncResult(id=taskid, app=app)
    if _res.state in ['SUCCESS', 'FAILURE']:
        return taskid, _res.state, _res.date_done.isoformat(), _res.result
    else:
        return taskid, _res.state,

def fetch_backend_taskresult(taskid: str) -> tuple:

    """
        Fetch task_id result
            - result needs to be deserialized
                result = pickle.loads(fetch_backend_taskresult({task_id})[0][0])
    """
    LOGGER.info(f'querying task: {taskid}')

    # connect to psql
    conn_response = connect_postgres()
    
    if conn_response["connection-status"]:

        # Creating a cursor object using the cursor() method
        conn = conn_response['conn']
        cursor = conn.cursor()

        # Executing an MYSQL function using the execute() method
        cursor.execute(f'select result from celery_taskmeta where task_id = \'{taskid}\' limit 10;')

        # Fetch a single row using fetchone() method.
        data = cursor.fetchall()
        LOGGER.info(f'data: {data}, type: {type(data)}, len: {len(data)}')

        # Closing the connection
        conn.close()
        return data
    else:
        LOGGER.info(f'connection failed: {conn_response}')
        raise Exception(f'Failed to pass test - {conn_response}')

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
        if len([r[0] for r in res if r[1] == 'RETRY']) == 0:
            break

    LOGGER.info(f'all tasks complete')
    return res

def hello_world(fpath: str, fpaths: str) -> None:

    """
        Submitting predefined tasks

            # sequential tasks
            data_files = generate_test_data(data_dir=data_dir)
            hello_world(fpath=data_files[0], fpaths=data_files)
    """

    LOGGER.info('starting tasks')

    # catch operational errors - perhaps cannot send message to worker
    try:

        # celery tasks expect serialized arguments, not objects - https://docs.celeryq.dev/en/stable/userguide/calling.html#serializers
        t1 = add.apply_async(args=[5, 7], queue='celery_template_queue')
        t2 = sort_list.apply_async(args=[fpath], queue='celery_template_queue')
        t3 = sort_directory.apply_async(args=[fpaths], queue='celery_template_queue')

        # [develop] save results once complete
        LOGGER.info(f'tasks submitted')

    except OperationalError as e: 
        LOGGER.error(f'app:{app} - failed to execute tasks - {e}')