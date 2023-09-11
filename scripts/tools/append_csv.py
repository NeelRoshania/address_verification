import argparse
import logging
import logging.config
from infusion_tools.funcs import append_files
from infusion_tools.sf import export_df

if __name__ == "__main__":

    """
        usage instructions
        - If multiple files are detected in the folder, they will all be appended together.
        - The schema's across these sets are assumed to be the same
        - If one file is detected, only that file will be processed 
        - Define location of the local csv output or optionally the SF output table
    
    """

    # parse arguments
    parser = argparse.ArgumentParser(description='append csv files in a folder then store locally or send to SF')
    parser.add_argument('directory',       type=str, help='local directory of files to append')
    parser.add_argument('-o', '--output_csv', default=None, help='local location to output operations')
    parser.add_argument('-osf', '--output_sf', default=None, help='snowflake table to export results')
    args = parser.parse_args()

    # setup logging environment
    logging.config.fileConfig('logs/logging.conf', defaults={'fileHandlerLog': 'logs/append_csv.log'})
    logger = logging.getLogger('main')

    # append csv
    if (args.output_csv is not None):

        # append csv's
        logger.info(f'----starting append_csv----')
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
        
        logger.info(f'----completed append_csv----')

    else:
        logger.error(f'script arguments not satisfied')
        raise IndexError('script arguments not satisfied')
