import threading


def printit():
    threading.Timer(30.0, printit).start()
    print("Hello, World!")


printit()
