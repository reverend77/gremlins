import platform
from multiprocessing import Process
from threading import Thread

def __is_cpython():
    return platform.python_implementation().lower() == "cpython"

Worker = Process if __is_cpython() else Thread
