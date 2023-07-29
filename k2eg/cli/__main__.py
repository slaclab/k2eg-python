# from k2eg.dml import k2eg
import os
import click
import logging
import k2eg.__version__ as __version__
import k2eg.cli.get as get
import k2eg.cli.monitor as monitor
from k2eg.dml import dml as k2eg
from click_loglevel import LogLevel
# import signal
# import sys
# import time

#global k2eg instance

k2eg_instance: k2eg = None

@click.group(chain=True)
@click.pass_context
@click.version_option(__version__.__version__, prog_name="K2EG demo cli")
@click.option('--environment', envvar='K2EG_CLI_DEFAULT_ENVIRONMENT', default='test')
@click.option(
    "-l", "--log-level",
    type=LogLevel(extra=["VERBOSE", "NOTICE"]),
    default=logging.INFO,
)
def cli(ctx, environment, log_level):
    global k2eg_instance
    logging.basicConfig(
        format="[%(levelname)-8s] %(message)s",
        level=log_level,
    )
    if os.environ.get('K2EG_PYTHON_CONFIGURATION_PATH_FOLDER') is None:
        config_path = "default embedded"
    else:
        config_path = os.environ.get('K2EG_PYTHON_CONFIGURATION_PATH_FOLDER')
    logging.debug("Use configuration '{}'".format(config_path))
    logging.debug("Use environment '{}'".format(environment))
    # allocate k2eg in 
    logging.debug("K2g initilizing")
    ctx.obj = k2eg_instance = k2eg(environment)
    pass

@cli.result_callback()
def process_pipeline(processors, environment, log_level):
    if k2eg_instance is not None:
        logging.debug("Deinit k2eg")
        k2eg_instance.close()
        logging.debug("Closed k2eg")

cli.add_command(get.get)
cli.add_command(monitor.monitor)
if __name__ == "__main__":
    cli()