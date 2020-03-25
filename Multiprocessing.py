import threading
import time

start = time.perf_counter()


def do_something(seconds):
    print(f'Sleeping {seconds} second(s)...')
    time.sleep(seconds)
    print('Done Sleeping...')


do_something(1)
do_something(1)

finish = time.perf_counter()

print('Finished in ' + str(round(finish-start, 2)) + ' second(s)')
