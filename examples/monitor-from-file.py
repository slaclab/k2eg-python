import logging
import sys
import os
import time
parent_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(parent_dir)
sys.path.insert(0, parent_dir)
import k2eg  # noqa: E402


def monitor_handler(pv_name, new_value):
    logging.info(new_value)


def example_monitor(k:k2eg, filename: str):
    logging.info('Monitor PV for 30 seconds')
    pv_list = []
    with open(filename, 'r') as file:
        # Iterate over each line in the file
        for line in file:
            logging.info(f"Add pv {line}")
            # Strip the newline character and append the line to the list
            pv_list.append(line.strip())
    if len(pv_list)==0:
        logging.info("No PV selected")
        return
    else:
        logging.info("Submit monitor for {len(pv_list)} PVs")

    r=k.monitor_many(pv_list, monitor_handler)
    start_time = time.time()
    end_time = start_time + 30
    while time.time() < end_time:
        time.sleep(1)

if __name__ == "__main__":
    k = None
    try:
        logging.basicConfig(
            format="[%(levelname)-8s] %(message)s",
            level=logging.DEBUG,
        )
        if len(sys.argv) == 1:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            filename = os.path.join(current_dir, "pv-names.txt")
        else:
            filename = sys.argv[1]
        logging.info(f"Monitor pv from file: {filename}")
        k = k2eg.dml('lcls', 'app-test')
        example_monitor(k, filename)
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