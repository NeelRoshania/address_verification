import psycopg2
import logging
from psycopg2 import OperationalError

from celery_template import cparser

LOGGER = logging.getLogger(__name__) # this calls the celery_template.funcs logger - which logs to worker node instance

def psql_connection(
    db: str,
    usr: str,    
    pswd: str,
    hst: str,
    prt: str
) -> any:
    LOGGER.info(f'connecting to psql {hst}:{prt}:{db}') # need to log this

    try:
        return {
            "connection-status": True,
            "conn": psycopg2.connect(
                                                database=db, 
                                                user=usr, 
                                                password=pswd, 
                                                host=hst, 
                                                port=prt
                                            )
        }
    except OperationalError as oe:
        return {
            "connection-status": False,
            "conn": None,
            "reason": oe
        }

def connect_postgres() -> dict:

    # establish connection without exposing configuration on high level
    return psql_connection(
                    db=cparser.get('postgresql_credentials', 'database'),
                    usr=cparser.get('postgresql_credentials', 'user'),        
                    pswd=cparser.get('postgresql_credentials', 'password'), 
                    hst=cparser.get('postgresql_credentials', 'host'), 
                    prt=cparser.get('postgresql_credentials', 'port')
        )