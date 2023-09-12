import argparse
import logging
import logging.config

from address_verification.funcs import specific_func

logging.config.fileConfig('conf/logging.conf', disable_existing_loggers=False, defaults={'fileHandlerLog': f'logs/{__name__}.log'})
LOGGER = logging.getLogger(__name__) # this will call the logger __main__ which will log to that referenced in python_template.__init__

def main(args):

    LOGGER.info(f'{__name__}: runscript started')
    specific_func(f'Module setup! (You shouldn\'t see this log on the console) - args:{args}')

def main_2(args):
    LOGGER.info(f'{__name__}: runscript completed')

if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=str, action="store", default="conf/config.yaml", nargs="?")
    parser.add_argument("--optional", "-o", action="store", type=str, default=8000)
    args = parser.parse_args()

    main(args)
    main_2(args)

