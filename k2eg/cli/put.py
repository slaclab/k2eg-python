import json
import logging
import click
from k2eg.dml import OperationTimeout, OperationError
import k2eg

@click.command()
@click.argument('pv_name', required=True)
@click.argument('value', required=True)
@click.option('--protocol', default='pva', help='The protocol pva,ca')
@click.option('--timeout', default=10, help='The timeout in seconds')

@click.pass_obj
def put(ctx_obj: dict, pv_name: str, value: str, protocol: str, timeout: int):
    """
    Execute a put operation from k2eg
    """
    try:
        ctx_obj.put(pv_name, value, protocol, timeout)
        click.echo("value applied")
    except OperationError as e:
        print(f"Remote error: {e.error} with message: {e.args[0]}")
    except OperationTimeout:
        print("Client timeout")
        pass
    except ValueError as e:
        print("Bad value {}".format(e.arg[0]))
        pass
