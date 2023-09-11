import argparse
import pandas as pd
import logging
import logging.config
from infusion_tools.funcs import split_df, get_yaml
from infusion_tools.sf import read_query, execute_query


if __name__ == "__main__":

    # Usage instrunctions
    # - Define the location of sf query, the local csv output or optionally the SF output table

    # parse arguments
    parser = argparse.ArgumentParser(description='Split data read locally or from SF into specified chunks')
    parser.add_argument('-o', '--output_loc', default=None, help='location to output operations')
    parser.add_argument('-q', '--query_loc', default=None, help='location to SnowFlake query')
    parser.add_argument('-c', '--csv_loc', default=None, help='location to local csv data')
    parser.add_argument('-s', '--split_info', default=None, help='location to yaml to read split configurations')
    args = parser.parse_args()

    # setup logging environment
    logging.config.fileConfig('logs/logging.conf', defaults={'fileHandlerLog': 'logs/read_set.log'})
    logger = logging.getLogger('main')

    # read set and split into files
    if (args.output_loc is not None):

        if (args.split_info is not None):
            
            # read from either sf table or csv file
            if (args.query_loc is not None) and ((args.csv_loc is not None)):
                logger.error(f'script arguments not satisfied')
                raise IndexError('script arguments not satisfied')
            
            # read from sf query
            if (args.query_loc is not None):
                data = execute_query(read_query(args.query_loc))

            # read from local csv file
            if (args.csv_loc is not None):
                data = pd.read_csv(args.csv_loc)

            # split rows
            logger.info(f'splitting rows: {len(data)}')
            param_split = get_yaml(args.split_info)
            res = split_df(
                args.output_loc,
                param_split["output_title"],
                param_split["chunks"],
                data
            )

            logger.info(f'summary of operations: {res}')

        else:
            logger.error(f'script arguments not satisfied')
            raise IndexError('script arguments not satisfied')
    else:
        logger.error(f'script arguments not satisfied')
        raise IndexError('script arguments not satisfied')