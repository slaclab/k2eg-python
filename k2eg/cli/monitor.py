import json
import click
from k2eg.dml import OperationTimeout, OperationError
import threading



@click.command()
@click.argument('pv_url')
@click.option('--timeout', default=10, help='The timeout in seconds')
@click.option('--filter', 
              default=[], multiple=True, 
              type=str, help='structure element to include')
@click.pass_obj
def monitor(ctx_obj: dict, pv_url: str, timeout: int, filter):
    """
    Execute a monitor operation from k2eg
    """
    evt = threading.Event()
    pv_name: str = None
    protocol: str = None
    def monitor_handler(pv_name, pv_value):
        nonlocal filter
        if len(filter)>0:
            pv_value = {key: pv_value[key] for key in filter}
        click.echo(json.dumps(pv_value, indent=4))
 
    try:
        protocol, pv_name = ctx_obj.parse_pv_url(pv_url)
        ctx_obj.monitor(pv_url, monitor_handler)
        evt.wait()
    except KeyboardInterrupt:
        evt.set()
    except OperationError as e:
        click.echo(f"Remote error: {e.error} with message: {e.args[0]}")
    except OperationTimeout:
        print("Client timeout")
        pass
    except ValueError as e:
        print(f"Bad value {e}")
        pass
    finally:
        ctx_obj.stop_monitor(pv_name)
