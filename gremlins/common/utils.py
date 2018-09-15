import platform


def is_cpython():
    return platform.python_implementation().lower() == "cpython"