import argparse
import subprocess
import logging

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", "-c", default=1)
    args = parser.parse_args()

    LOGGER = logging.getLogger(__name__) # this logger is defined seperately, see logging.conf

    # run celery worker as a subprocess
    LOGGER.info(f'starting celery worker node - app:celery_template, queue: celery_template_queue, worker:worker')
    subprocess.Popen(
                        [
                            "celery",
                            "-A",
                            "address_verification",
                            "worker",
                            "-Q",
                            "address_verification_queue",
                            "--loglevel=INFO",
                            f'--concurrency={args.concurrency}',
                            # "--logfile=logs/celery.log"
                        ]
                    )
    
    # # starting a flower instance
    # celery flower --port=5566
