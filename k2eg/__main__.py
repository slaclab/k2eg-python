from k2eg import cli
from k2eg.k2eg import k2eg
import typer
from typing import Optional

app = typer.Typer()

@app.command()
def get(pv_name: str, protocol: str):
    """
    Execute a get operation from k2eg
    """
    k = k2eg()
    try:
        r = k.get(pv_name, protocol)
        print(r)
    finally:
        k.close()
    


@app.command()
def bye(name: Optional[str] = None):
    if name:
        typer.echo(f"Bye {name}")
    else:
        typer.echo("Goodbye!")

if __name__ == "__main__":
    app()