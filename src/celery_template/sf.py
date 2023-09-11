import pandas as pd
import snowflake.connector as sf
import os

from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import pd_writer
from snowflake.connector import SnowflakeConnection

from celery_template import logger

def _get_sf_options() -> dict:
    return {
     'user' :       os.getenv('SF_USER'), 
     'password':    os.getenv('SF_PASSWORD'),
     'warehouse':   os.getenv('SF_WAREHOUSE'),
     'schema':      os.getenv('SF_SCHEMA'),
     'database':    os.getenv('SF_DATABASE'),
     'role': 'DATA_STRATEGY',
     'account' : 'discoverorg'
    }

def sf_connection(
    database: str = _get_sf_options()['database'],
    schema: str = _get_sf_options()['schema'],
    account: str = _get_sf_options()['account'],
    warehouse: str = _get_sf_options()['warehouse'],
) -> SnowflakeConnection:
    logger.info(f'authenticating snowflake credentials and reading query')
    return sf.connect(
        user=_get_sf_options()['user'],
        password=_get_sf_options()['password'],
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )

def snowflake(query, options) -> pd.DataFrame:
    logger.info(f'establishing snowflake connection')
    _ctx = sf_connection()
    _df = pd.read_sql(query, _ctx)
    return _df

def read_query(query_loc) -> str:
    logger.info(f'reading query from {query_loc}')
    with open(query_loc) as f:
        fcontents = f.read()
    return fcontents

def execute_query(sf_query) -> pd.DataFrame:
    logger.info(f'executing query specified')
    sf_cred = _get_sf_options()
    return snowflake(sf_query, sf_cred)

def export_df(_df, sf_export_table) -> dict:

    # https://docs.snowflake.com/en/user-guide/sqlalchemy.html
    try:

        # convert columns to uppercase and reset_index
        logger.info(f'exporting results to {sf_export_table}')
        _df = _df.reset_index(drop=False)
        _df.columns = [str.upper(col) for col in _df.columns]

        # write to sf
        _sf = create_engine(URL(**_get_sf_options()))
        _df.to_sql(
            name=f'{sf_export_table}',
            con=_sf,
            index=False,
            method=pd_writer,
            if_exists='replace'
        )

        # log and return
        logger.info(f'set export completed')
        logger.info(f'{len(_df)} records exported')
        logger.info(f'set schema: {_df.columns}')
        return {
            'result': 'complete', 
            'records_exported': len(_df),
            'schema': _df.columns
            }
    except Exception as e:
        logger.error(f'set export failed: {e}')
        return {
            'result': 'failed', 
            'error': e
            }
    finally:
        _sf.dispose()