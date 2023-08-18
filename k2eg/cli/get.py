import json
import logging
import click
from k2eg.dml import OperationTimeout, OperationError
import k2eg

@click.command()
@click.argument('pv_name', required=True)
@click.option('--protocol', default='pva', help='The protocol pva,ca')
@click.option('--timeout', default='10', help='The timeout in seconds')
@click.option('--filter', default=[], multiple=True, type=str, help='structure element to include')
@click.pass_obj
def get(k2eg: k2eg, pv_name: str, protocol: str, timeout: int, filter):
    """
    Execute a get operation from k2eg
    """
    #k2eg.with_for_backends()
    logging.debug("GET on {} with protocol {}".format( pv_name, protocol))
    try:
        result = k2eg.get(pv_name, protocol)
        if len(filter)>0:
            result = {key: result[key] for key in filter}
        click.echo(json.dumps(result, indent=4))
    except OperationError as e:
        print("Remote error: {} with message: {}".format(e.error,e.args[0]))
    except OperationTimeout as e:
        print("Client timeout")
        pass
    except ValueError as e:
        print("Bad value {}".format(e.arg[0]))
        pass
