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
from multiprocessing.dummy import Pool

"""

    Celery task execution
        - single tasks
        - grouped tasks

        - timed executions

"""

# logging configurations
logging.config.fileConfig('conf/logging.conf', defaults={'fileHandlerLog': f'logs/{__name__}.log'})

LOGGER = logging.getLogger(__name__) # this will call the logger __main__ which will log to that referenced in python_template.__init__

def retry_tasks(job_id: str) -> None:

    """
        Submitting predefined tasks with retry logic if failed
        
    """

    tasks = []

    try:

        # generate tasks
        for i in enumerate(range(10)):
            tasks.append(
                [
                    datetime.utcnow().isoformat(),
                    geopy_verify_address.apply_async(args=[random.random()], queue='address_verification_queue').id
                ]
            )
            # break

        LOGGER.info(f'{job_id} - tasks submitted')

        return tasks

    except OperationalError as e: 
        LOGGER.error(f'app:{app} - failed to execute tasks - {e}')

def job_handler(job_id: str) -> tuple:

    """
        Submit jobs of predefined tasks

    """
    try:
        LOGGER.info(f'{job_id} - starting job')
        
        # submit tasks
        tasks_submitted = retry_tasks(job_id=job_id)
        taskids = [task[1] for task in tasks_submitted]
        write_csv(file_loc=f'tests/data/results/jobs/submitted/{job_id}.csv', data=list(tasks_submitted)) # [date_started, task_id]
        
        # check for results - not really the best way to track this
        res = await_tasks_completion(taskids=taskids) # blocking
        write_csv(file_loc=f'tests/data/results/jobs/completed/{job_id}.csv', data=list(res))  # [taskid, state, date_done, result]

    except OperationalError as e: 
        LOGGER.error(f'app:{app} - failed to execute tasks - {e}')

if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=str, action="store", default="conf/config.yaml", nargs="?")
    parser.add_argument("--optional", "-o", action="store", type=str, default=8000)
    args = parser.parse_args()

    data_dir = r'data/addresses.csv'

    # submit 3 jobs of 10 tasks per job - nned to figure out how to handle this, maybe chunk the records for verification
    jobs = [str(uuid.uuid1()) for i in range(3)]
    for jobid in jobs:
        job_handler(jobid)
        break
