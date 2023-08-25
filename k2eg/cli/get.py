import json
import click
from k2eg.dml import OperationTimeout, OperationError

@click.command()
@click.argument('pv_url', required=True)
@click.option('--timeout', default=10, help='The timeout in seconds')
@click.option('--filter', 
              default=[], multiple=True, 
              type=str, help='structure element to include')
@click.pass_obj
def get(ctx_obj: dict, pv_url: str, timeout: int, filter):
    """
    Execute a get operation from k2eg
    """
    try:
        result = ctx_obj.get(pv_url, timeout)
        if len(filter)>0:
            result = {key: result[key] for key in filter}
        click.echo(json.dumps(result, indent=4))
    except OperationError as e:
        print(f"Remote error: {e.error} with message: {e.args[0]}")
    except OperationTimeout:
        print("Client timeout")
        pass
    except ValueError as e:
        print("Bad value {}".format(e))
        pass
