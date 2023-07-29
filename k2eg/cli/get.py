import click
from k2eg import dml as K2eg
from k2eg.dml import OperationTimeout, OperationError
import k2eg

@click.command()
@click.argument('pv_name', required=True)
@click.option('--protocol', default='pva', help='The protocol pva,ca')
@click.pass_obj
def get(k2eg: k2eg, pv_name: str, protocol: str):
    """
    Execute a get operation from k2eg
    """
    #k2eg.with_for_backends()
    click.echo("GET on {} with protocol {}".format( pv_name, protocol))
    try:
        r = k2eg.get(pv_name, protocol)
        print(r)
    except OperationError as e:
        pass
    except OperationTimeout as e:
        pass
    except ValueError as e:
        pass


# def signal_handler(sig, frame):
#     print('You pressed Ctrl+C!')
#     sys.exit(0)
