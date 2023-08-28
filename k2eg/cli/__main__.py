# from k2eg.dml import k2eg
import os
import click
import logging
import k2eg.__version__ as __version__
import k2eg.cli.get as get
import k2eg.cli.put as put
import k2eg.cli.monitor as monitor
import k2eg
from click_loglevel import LogLevel
from click_repl import repl
from click_repl import register_repl

k2eg_dml_instance: k2eg.dml = None
initilized: bool = False
in_shell: bool = False

@click.group(chain=True, invoke_without_command=True)
@click.pass_context
@click.version_option(__version__.__version__, prog_name="K2EG demo cli")
@click.option('--environment', envvar='K2EG_CLI_DEFAULT_ENVIRONMENT', default='test')
@click.option(
    "-l", "--log-level",
    type=LogLevel(extra=["VERBOSE", "NOTICE"]),
    default=logging.INFO,
)
def cli(ctx, environment, log_level):
    global k2eg_dml_instance
    global initilized
    global in_shell
    if not initilized:
        ctx.obj = {}
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
        ctx.obj = k2eg_dml_instance = k2eg.dml(environment, 'demo-cli')
        initilized = True
    
    if ctx.invoked_subcommand is None:
        in_shell = True
        repl(ctx)
        print('Exing shell')
        in_shell = False
        
    pass

@cli.result_callback()
def process_pipeline(processors, environment, log_level):
    if k2eg_dml_instance is not None and in_shell is False:
        logging.debug("Deinit dml")
        k2eg_dml_instance.close()
        logging.debug("Closed dml")

register_repl(cli)  # Register the REPL command
cli.add_command(get.get)
cli.add_command(put.put)
cli.add_command(monitor.monitor)
if __name__ == "__main__":
    cli()