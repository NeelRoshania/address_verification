import pytest
import logging

from address_verification.csv import write_csv
from address_verification.funcs import generate_test_data
from address_verification.tasks import sort_list, sort_directory

# usage: 
#   - pytest tests/scripts/test_sample.py
#   - pytest -v

LOGGER = logging.getLogger(__name__)
test_dir = 'tests/data'


def test_sort_row():
    tdata_files = generate_test_data(test_dir)

    res = sort_list(fpath=f'{test_dir}/testdata_singlelist_021923.csv')

    LOGGER.info(f'test complete - {res}') # doesn't work yet
    write_csv(file_loc=f'{test_dir}/results/test_sort_row.csv', data=res["data"])

    assert res["success"] == True

def test_sort_manyrows():
    
    tdata_files = generate_test_data(test_dir)

    res = sort_list(fpath=f'{test_dir}/testdata_many_021923.csv')

    LOGGER.info(f'test complete - {res}') # doesn't work yet
    write_csv(file_loc=f'{test_dir}/results/test_sort_manyrows.csv', data=res["data"])

    assert res["success"] == True

def test_sort_directory():
    
    tdata_files = generate_test_data(test_dir)

    res = sort_directory(fpaths=tdata_files)

    LOGGER.info(f'test complete - {res}') # doesn't work yet
    write_csv(file_loc=f'{test_dir}/results/test_sort_directory.csv', data=res["data"])

    assert res["success"] == True