import click

@click.command()
@click.argument('pv_name')
@click.option('--protocol', default='pva', help='The protocol pva,ca')
@click.option('--timeout', default='pva', help='The timeout in seconds')
def monitor(pv_name: str, protocol: str, timeout: int = 10):
    """
    Execute a monitor operation from k2eg
    """
    click.echo("MONITOR on {} with protocol {}".format( pv_name, protocol))
    # exit_flag = False
    # k = k2eg()

    # def monitor_handler(new_value):
    #     print(new_value)
    
    # def signal_handler(sig, frame):
    #     exit_flag = True

    # signal.signal(signal.SIGINT, signal_handler)
    # signal.signal(signal.SIGTERM, signal_handler)
    
    # try:
    #     k.monitor(pv_name, protocol, monitor_handler)
    #     time.sleep(timeout_second)
    #     k.stop_monitor(pv_name)
    # finally:
    #     k.close()