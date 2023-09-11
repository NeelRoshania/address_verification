import argparse
import logging
import logging.config
from infusion_tools.funcs import append_files
from infusion_tools.sf import export_df

if __name__ == "__main__":

    # usage instructions
    # - If multiple files are detected in the folder, they will all be appended together.
    # - The schema's across these sets are assumed to be the same
    # - If one file is detected, only that file will be processed 

    # parse arguments
    parser = argparse.ArgumentParser(description='Append and send file or files in a local folder to snowflake')
    parser.add_argument('directory',       type=str, help='location of local file directory')
    parser.add_argument('-o', '--output_csv', default=None, help='location to output operations')
    parser.add_argument('-osf', '--output_sf', default=None, help='SnowFlake table to export results')
    args = parser.parse_args()

    # setup logging environment
    logging.config.fileConfig('logs/logging.conf', defaults={'fileHandlerLog': 'logs/export_set.log'})
    logger = logging.getLogger('main')

    # export set
    if (args.output_csv is not None):

        # append csv's
        logger.info(f'----starting export_set----')
        df_append = append_files(args.directory)

        # output to csv
        df_append.to_csv(
                    args.output_csv,
                    index=False,
                    encoding='utf-8-sig'
                    )
        logger.info(f'data exported locally to {args.output_csv}')

        # optional export to sf
        if (args.output_sf is not None):
            res = export_df(df_append, args.output_sf)
        
        logger.info(f'----completed export_set----')

    else:
        logger.error(f'script arguments not satisfied')
        raise IndexError('script arguments not satisfied')
