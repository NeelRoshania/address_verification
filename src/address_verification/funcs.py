import datetime as dt
import logging
import math
import os
import random

from address_verification.csv import write_csv
from address_verification.psql import psql_connection

LOGGER = logging.getLogger(__name__) # this calls the celery_template.funcs logger - which logs to worker node instance

def specific_func(text:str) -> None:

    """
        Service to....

    """

    LOGGER.info(f'{text}')

    return None

def get_duration(start_time, end_time) -> float:

    """
        Calculate duration in seconds
        start_time:  time in seconds since the epoch as a floating point number
        end_time:  time in seconds since the epoch as a floating point number
    """
    return f'{end_time-start_time:.2f}'

def exponential_backoff(n0:int, retries: int) -> dt.datetime:

    """
        seconds to retrying the next task

    """
    # return (dt.datetime.now() + dt.timedelta(seconds=int(math.exp(retries))))
    return random.randint(0, n0**(2*retries))

def generate_test_data(data_dir: str) -> None:

    """
        No reservation made to captures missing directories

    """
    import numpy as np

    LOGGER.info('generating test data')

    values_one = [[0, np.random.randint(1000, size=int(5e3)).tolist()]]
    values_many = [[i[0], np.random.randint(1000, size=int(5e3)).tolist()] for i in enumerate(range(10))]

    write_csv(
        file_loc=f'{data_dir}/testdata_singlelist_021923.csv', 
        data=values_one, 
        schema=['record_id', 'data']
    )

    write_csv(
        file_loc=f'{data_dir}/testdata_many_021923.csv', 
        data=values_many, 
        schema=['record_id', 'data']
    )

    return [f'{data_dir}/{f}' for f in os.listdir('tests/data') if f.endswith('.csv')]
