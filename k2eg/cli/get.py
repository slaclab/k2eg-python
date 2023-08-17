import click
from k2eg.dml import OperationTimeout, OperationError
import k2eg

@click.command()
@click.argument('pv_name', required=True)
@click.option('--protocol', default='pva', help='The protocol pva,ca')
@click.option('--timeout', default='10', help='The timeout in seconds')
@click.pass_obj
def get(k2eg: k2eg, pv_name: str, protocol: str, timeout: int):
    """
    Execute a get operation from k2eg
    """
    #k2eg.with_for_backends()
    click.echo("GET on {} with protocol {}".format( pv_name, protocol))
    try:
        r = k2eg.get(pv_name, protocol)
        print(r)
    except OperationError as e:
        print("Remote error: {} with message: {}".format(e.error,e.args[0]))
    except OperationTimeout as e:
        print("Client timeout")
        pass
    except ValueError as e:
        print("Bad value {}".format(e.arg[0]))
        pass
