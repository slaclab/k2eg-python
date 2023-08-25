import click
from k2eg.dml import OperationTimeout, OperationError

@click.command()
@click.argument('pv_url', required=True)
@click.argument('value', required=True)
@click.option('--timeout', default=10, help='The timeout in seconds')

@click.pass_obj
def put(ctx_obj: dict, pv_url: str, value: str, timeout: int):
    """
    Execute a put operation from k2eg
    """
    try:
        ctx_obj.put(pv_url, value, timeout)
        click.echo("value applied")
    except OperationError as e:
        print(f"Remote error: {e.error} with message: {e.args[0]}")
    except OperationTimeout:
        print("Client timeout")
        pass
    except ValueError as e:
        print("Bad value {}".format(e))
        pass
