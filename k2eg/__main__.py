from k2eg import cli
from k2eg.k2eg import k2eg
import typer
from typing import Optional
import signal
import sys
import time

app = typer.Typer()

@app.command()
def get(pv_name: str, protocol: str = 'pva'):
    """
    Execute a get operation from k2eg
    """
    k = k2eg()
    try:
        r = k.get(pv_name, protocol)
        print(r)
    finally:
        k.close()

def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)

@app.command()
def monitor(pv_name: str, protocol: str = 'pva', timeout_second: int = 10):
    """
    Execute a monitor operation from k2eg
    """
    exit_flag = False
    k = k2eg()

    def monitor_handler(new_value):
        print(new_value)
    
    def signal_handler(sig, frame):
        exit_flag = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        k.monitor(pv_name, protocol, monitor_handler)
        time.sleep(timeout_second)
        k.stop_monitor(pv_name)
    finally:
        k.close()

if __name__ == "__main__":
    app()