from k2eg import cli
import curses
import threading
import time

def tail_thread(text_window):
    for i in range(100):
        # If it's time to scroll
        if i > text_window.getmaxyx()[0] - 1:
            text_window.scroll()

        # Add the new line at the bottom
        text_window.move(text_window.getmaxyx()[0] - 1, 0)
        text_window.clrtoeol()
        text_window.addstr(text_window.getmaxyx()[0] - 1, 0, f"This is line {i}")
        text_window.refresh()
        time.sleep(1)

def main(stdscr):
    # Clear screen
    stdscr.clear()
    
    # Get the size of the window
    sh, sw = stdscr.getmaxyx()

    # Create the windows
    w1 = curses.newwin(sh // 2, sw, 0, 0)
    w2 = curses.newwin(sh // 2, sw, sh // 2, 0)
    w1.scrollok(True)
    thread = threading.Thread(
            target=tail_thread(w1)
    )
    thread.start()
    # Let the bottom window listen to the user's input
    while True:
        w2.clear()  # Clear the bottom window
        w2.addstr("Enter a command: ")

        # Get the command from the user
        command = w2.getstr().decode('utf-8') 

        # If the user entered 'exit', then quit the loop
        if command == 'exit':
            break

        # # Display the result in the top window
        # w1.clear()  # Clear the top window
        # w1.addstr(f"Command entered: {command}")

        # Refresh the windows to display the changes
        w1.refresh()
        w2.refresh()

if __name__ == "__main__":
    curses.wrapper(main)