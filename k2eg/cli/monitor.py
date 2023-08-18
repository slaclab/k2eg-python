import json
import logging
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
@click.option('--filter', 
              default=[], multiple=True, 
              type=str, help='structure element to include')
@click.pass_obj
def monitor(k2eg: k2eg, pv_name: str, protocol: str, timeout: int, filter):
    """
    Execute a monitor operation from k2eg
    """
    logging.debug("MONITOR on {pv_name} with protocol {protocol}")

    def monitor_handler(new_value_dic):
        nonlocal filter
        nonlocal pv_name
        if pv_name in new_value_dic:
            pv_value = new_value_dic[pv_name]
            if len(filter)>0:
                pv_value = {key: pv_value[key] for key in filter}
            click.echo(json.dumps(pv_value, indent=4))
        else:
            logging.error(f'Invalid key found on monitor package for {pv_name}')

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        k2eg.monitor(pv_name, monitor_handler, protocol)
        evt.wait()
    except OperationError as e:
        click.echo(f"Remote error: {e.error} with message: {e.args[0]}")
    except OperationTimeout:
        print("Client timeout")
        pass
    except ValueError as e:
        print(f"Bad value {e.arg[0]}")
        pass
    finally:
        k2eg.stop_monitor(pv_name)

def signal_handler(sig, frame):
    evt.set()