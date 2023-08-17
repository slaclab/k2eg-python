import click
import signal
from k2eg.dml import OperationTimeout, OperationError
import k2eg
import threading

evt = threading.Event()

@click.command()
@click.argument('pv_name')
@click.option('--protocol', default='pva', help='The protocol pva,ca')
@click.option('--timeout', default='10', help='The timeout in seconds')
@click.pass_obj
def monitor(k2eg: k2eg, pv_name: str, protocol: str, timeout: int):
    """
    Execute a monitor operation from k2eg
    """
    click.echo("MONITOR on {} with protocol {}".format( pv_name, protocol))

    def monitor_handler(new_value):
        print(new_value)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        k2eg.monitor(pv_name, monitor_handler, protocol)
        evt.wait()
    except OperationError as e:
        print("Remote error: {} with message: {}".format(e.error,e.args[0]))
    except OperationTimeout as e:
        print("Client timeout")
        pass
    except ValueError as e:
        print("Bad value {}".format(e.arg[0]))
        pass
    finally:
        k2eg.stop_monitor(pv_name)

def signal_handler(sig, frame):
    evt.set()