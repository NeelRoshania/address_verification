import configparser
import logging
import logging.config
import snowflake.connector

"""
    Logic to test sf connection
    
    - Setup guide, https://docs.snowflake.com/en/user-guide/python-connector-install.html#step-2-verify-your-installation
    - Working guide
        - Setup virtual environment
        - pip install -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.7.3/tested_requirements/requirements_39.reqs
        - python tests/scripts/test_sf_connection.py
        
"""

# setup config parser config_parser
cparser = configparser.ConfigParser()
cparser.read('conf/tools.conf')

# setup logging environment
logging.config.fileConfig('logs/logging.conf', defaults={'fileHandlerLog': 'logs/test_sf_connection.log'})
logger = logging.getLogger('main')

# Test passes if connector is able to query and log the version
logger.info(f'starting connection')
ctx = snowflake.connector.connect(
        user=cparser.get('snowflake_credentials', 'user'),
        password=cparser.get('snowflake_credentials', 'password'),
        account=cparser.get('snowflake_credentials', 'account'),
        warehouse=cparser.get('snowflake_credentials', 'warehouse'),
        database=cparser.get('snowflake_credentials', 'database'),
        schema=cparser.get('snowflake_credentials', 'schema')
    )
cs = ctx.cursor()

logger.info(f'running query')

# test connection
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    logger.info(f'Snowflake version: {one_row[0]}')
except:
    logger.error(f'failed to execute query')
finally:
    logger.info(f'query complete')
    cs.close()

ctx.close()