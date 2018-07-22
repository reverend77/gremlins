import pika
from gremlins.common.task_distribution import TaskPublisher, TaskSubscriber
from multiprocessing import Process
from random import randint
from gremlins.common.constant_values import CLIENT_HOSTNAME
import socket


def start_publisher(ip):
    connection_in = pika.BlockingConnection(pika.ConnectionParameters(ip))
    connection_out = pika.BlockingConnection(pika.ConnectionParameters(ip))
    publisher = TaskPublisher(connection_in, connection_out)
    publisher.start()

    return publisher


def start_subscriber(ip):
    connection2 = pika.BlockingConnection(pika.ConnectionParameters(ip))
    subscriber = TaskSubscriber(connection2)
    subscriber.start()


if __name__ == "__main__":

    client_hostname = socket.gethostname()  # CLIENT_HOSTNAME
    ip = socket.gethostbyname(client_hostname)

    publisher = start_publisher(ip)
    subscriber_proc = [Process(target=start_subscriber, args=[ip]) for __ in range(3)]
    [proc.start() for proc in subscriber_proc]

    while True:
        hook = publisher.submit_task("rotfl", [randint(0, 100), randint(0, 100)])
        print(hook())

