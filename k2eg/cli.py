from typing import Optional

import typer

app = typer.Typer()

def _version_callback(value: bool) -> None:
    if value:
        typer.echo("k2eg v01")
        raise typer.Exit()

@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        help="Show the application's version and exit.",
        callback=_version_callback,
        is_eager=True,
    )
) -> None:
    return