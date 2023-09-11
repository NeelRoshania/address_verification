import logging
import csv

from csv import Error

LOGGER = logging.getLogger(__name__)

def write_text(file_loc: str, data: str, encoding:str = None) -> None:

    """

        write_text(file_loc: str, data: str)  -> None
            - write string data to file

    """
    
    # handling optional arguments
    data = "" if data is None else data
    encoding = 'utf-8' if encoding is None else encoding

    try:
        with open(file_loc, "w", encoding=encoding) as f:
            f.write(data)
        LOGGER.info(f'text data written to {file_loc}')
    except Exception as e:
        LOGGER.debug(f'unexpected exception - failed to write to {file_loc}: {e}')
        raise Exception(e)

def write_csv(file_loc:str, data: list[list], schema: list = None, encoding:str = None) -> None:

    """

        write_csv(file_loc:str, data: list) -> None
            - write comma seperated list objects
            - data: request list of lists

        write_csv(file_loc=f'{}.csv', data=[list], schema=[schema])

    """

    # handle optional arguments
    data = [] if data is None else data
    encoding = 'utf-8-sig' if encoding is None else encoding

    try:

        with open(file_loc, "w", newline='', encoding=encoding) as f:
            writer = csv.writer(f, delimiter=",")
            if schema is not None:
                writer.writerow(header for header in schema)
                for row in data:
                    writer.writerow(row)
            else:
                writer.writerows(data)
        LOGGER.info(f'data written to {file_loc}, encoding: {encoding}')
    except Error as e:
        LOGGER.debug(f'failed to write to {file_loc}: {e}')
        raise TypeError(f'csv error - {e}')
    except Exception as e:
        LOGGER.debug(f'unexpected exception - failed to write to {file_loc}: {e}')
        raise Exception(e)

def read_text(file_loc: str, encoding:str = None) -> str:

    """

        read_text(file_loc: str) -> None
            - read text data

    """

    try:

        encoding = 'utf-8-sig' if encoding is None else encoding
        with open(file_loc, "r", encoding=encoding) as f:
            _data = f.read()
        LOGGER.info(f'data read from {file_loc}')
    
        return _data
    
    except Exception as e:
        LOGGER.debug(f'failed to read {file_loc}: {e}')
        raise Exception(e)
        
def read_csv(file_loc:str, encoding:str = None, delimiter:str = None) -> list:

    """
    
        read_csv(file_loc:str, encoding:str = None, delimiter:str = None) -> list:
            - write comma seperated list objects
            - returns list of lists:
                - l = read_csv('file_loc.csv') [[A], [B], [C]] or [[headers], [[data]]]
                    - with headers: headers, *data = read_csv(...)
                - lflat = [line[0] for line in read_csv('file_loc.csv')] # lflat = [A, B, C] given data=[[1], [2], [3]...,[n]]

        - considerations
            - although the result is a list object, nested objects are returned as strings
                - json.loads

    """

    # handle optional arguments
    encoding = 'utf-8-sig' if encoding is None else encoding
    delimiter = ',' if delimiter is None else delimiter
    _data = []

    try:
        
        # read csv and return list data object
        with open(file_loc, newline='', encoding=encoding) as f:
            reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                _data.append(row)
 
        LOGGER.info(f'data read from {file_loc}, encoding: {"defualt" if encoding is None else encoding}, delimiter: {delimiter}')
        
        return _data
    
    except Exception as e:
        LOGGER.debug(f'failed to read csv from {file_loc}: {e}')
        raise Exception(e)