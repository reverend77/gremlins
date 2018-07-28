# main function wrapper
import sys
from gremlins.server import server_main
from gremlins.client import client_main

if __name__ == "__main__":
    args = sys.argv
    if len(args) != 2:
        print("Unknown arguments: {}".format(args[1:]))
    else:
        mode = args[1]
        if mode == "s":
            server_main.main()
        elif mode == "c":
            client_main.main()
        else:
            print("Unknown mode, quitting.")
