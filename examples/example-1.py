import logging
import sys
import os
import time

parent_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(parent_dir)
sys.path.insert(0, parent_dir)

import k2eg

def example_get(k:k2eg):
    logging.info('Get the PV')
    r=k.get('ca://VGXX:L3B:1602:PLOG')
    logging.info(r)

def monitor_handler(pv_name, new_value):
    logging.info(new_value)


def example_monitor(k:k2eg):
    logging.info('Monitor PV for 30 seconds')
    k.monitor('ca://VGXX:L3B:1602:PLOG', monitor_handler)
    logging.info('Stop Monitor PV')
    start_time = time.time()
    end_time = start_time + 30
    while time.time() < end_time:
        time.sleep(1)
    k.stop_monitor('VGXX:L3B:1602:PLOG')
    

if __name__ == "__main__":
    k = None
    try:
        logging.basicConfig(
            format="[%(levelname)-8s] %(message)s",
            level=logging.DEBUG,
        )
        k = k2eg.dml('lcls', 'app-test')
        example_get(k)
        example_monitor(k)
    except k2eg.OperationError as e:
        print(f"Remote error: {e.error} with message: {e.args[0]}")
    except k2eg.OperationTimeout:
        print("Operation timeout")
        pass
    except ValueError as e:
        print("Bad value {}".format(e))
        pass
    except  TimeoutError as e:
        print("Client timeout")
        pass

    finally:
        if k is not None:
            k.close()
