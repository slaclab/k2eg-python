# from k2eg.dml import k2eg
import click
import k2eg.__version__ as __version__
# import signal
# import sys
# import time

@click.group()
@click.pass_context
@click.version_option(__version__.__version__, prog_name="K2EG demo cli")
def cli(ctx):
    pass

@click.command()
@click.argument('pv_name')
@click.option('--protocol', default='pva', help='The protocol pva,ca')
def get(pv_name: str, protocol: str):
    """
    Execute a get operation from k2eg
    """
#     k = k2eg()
#     try:
#         r = k.get(pv_name, protocol)
#         print(r)
#     finally:
#         k.close()

# def signal_handler(sig, frame):
#     print('You pressed Ctrl+C!')
#     sys.exit(0)

@click.command()
@click.argument('pv_name')
@click.option('--protocol', default='pva', help='The protocol pva,ca')
@click.option('--timeout', default='pva', help='The timeout in seconds')
def monitor(pv_name: str, protocol: str, timeout: int = 10):
    """
    Execute a monitor operation from k2eg
    """
    # exit_flag = False
    # k = k2eg()

    # def monitor_handler(new_value):
    #     print(new_value)
    
    # def signal_handler(sig, frame):
    #     exit_flag = True

    # signal.signal(signal.SIGINT, signal_handler)
    # signal.signal(signal.SIGTERM, signal_handler)
    
    # try:
    #     k.monitor(pv_name, protocol, monitor_handler)
    #     time.sleep(timeout_second)
    #     k.stop_monitor(pv_name)
    # finally:
    #     k.close()

cli.add_command(get)
cli.add_command(monitor)

if __name__ == "__main__":
    cli()