import argparse
import logging
import logging.config
import random
import uuid

from datetime import datetime
from address_verification import app
from address_verification.tasks import geopy_verify_address, await_tasks_completion
from address_verification.csv import read_csv, write_csv
from kombu.exceptions import OperationalError

"""

    Celery task execution
        - single tasks
        - grouped tasks

        - timed executions

"""

# logging configurations
logging.config.fileConfig('conf/logging.conf', defaults={'fileHandlerLog': f'logs/{__name__}.log'})

LOGGER = logging.getLogger(__name__) # this will call the logger __main__ which will log to that referenced in python_template.__init__

def retry_tasks(job_id: str, address_data: list) -> None:

    """
        Submitting predefined tasks with retry logic if failed
        
    """

    tasks = []

    try:

        # submit address verification task requests
        while True:
            try:
                tasks.append(
                [
                    datetime.utcnow().isoformat(),
                    geopy_verify_address.apply_async(args=[next(address_data)[11]], queue='address_verification_queue').id
                ]
            )
            except StopIteration as e:
                LOGGER.info('tasks submitted')
                break

        LOGGER.info(f'{job_id} - tasks submitted')

        return tasks

    except OperationalError as e: 
        LOGGER.error(f'app:{app} - failed to execute tasks - {e}')

def job_handler(job_id: str, data: list) -> tuple:

    """
        Submit jobs of predefined tasks
    """
    try:
        LOGGER.info(f'{job_id} - starting job')
        
        # submit tasks
        tasks_submitted = retry_tasks(job_id=job_id, address_data=data)
        taskids = [task[1] for task in tasks_submitted]
        write_csv(file_loc=f'data/results/jobs/submitted/{job_id}.csv', data=list(tasks_submitted)) # [date_started, task_id]
        
        # check for results - not really the best way to track this
        res = await_tasks_completion(taskids=taskids) # blocking
        write_csv(file_loc=f'data/results/jobs/completed/{job_id}.csv', data=list(res))  # [taskid, state, date_done, result]

    except OperationalError as e: 
        LOGGER.error(f'app:{app} - failed to execute tasks - {e}')

if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=str, action="store", default="conf/config.yaml", nargs="?")
    parser.add_argument('addresses', type=str, action="store", help='csv file of addresses for geopy_verification')
    args = parser.parse_args()

    data_dir = args.addresses
    d = (r for r in read_csv(data_dir)[1::])
    d_headers = read_csv(data_dir)[0]

    # issue tasks
    # emails = (_d[0] for _d in d)
    # generated_addresses = (_d[11] for _d in d)

    job_handler(job_id=uuid.uuid1(), data=d)
