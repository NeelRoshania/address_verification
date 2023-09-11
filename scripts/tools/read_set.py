import argparse
import logging
import logging.config
from infusion_tools.sf import read_query, execute_query


if __name__ == "__main__":

    # Usage instructions
    # - Define the location of sf query, the local csv output or optionally the SF output table

    # parse arguments
    parser = argparse.ArgumentParser(description='Read data from SF and download locally')
    parser.add_argument('-o', '--output_loc', default=None, help='location to output operations')
    parser.add_argument('-q', '--query_loc', default=None, help='location to SnowFlake query')
    args = parser.parse_args()

    # setup logging environment
    logging.config.fileConfig('logs/logging.conf', defaults={'fileHandlerLog': 'logs/read_set.log'})
    logger = logging.getLogger('main')

    # read set
    if (args.output_loc is not None) or (args.query_loc is not None):
            
            # read from sf query and download locally
            logger.info(f'----starting read_set----')
            if (args.query_loc is not None):
                try:
                    data = execute_query(read_query(args.query_loc))
                    data.to_csv(
                        args.output_loc, 
                        encoding='utf-8-sig',
                        index=False
                        )
                    logger.info(f'export location: {args.output_loc}')
                    logger.info(f'rows exported: {args.output_loc}')
                    logger.info(f'schema: {data.columns}')
                except Exception as e:
                    logger.fail(f'failed to read or download sets locally: {e}')

    else:
        logger.error(f'script arguments not satisfied')
        raise IndexError('script arguments not satisfied')