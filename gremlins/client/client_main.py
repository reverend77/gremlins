import socket
from multiprocessing import Process

import pika

from gremlins.common.task_distribution import TaskPublisher, TaskSubscriber, TaskDivider


def start_publisher(ip):
    connection_in = pika.BlockingConnection(pika.ConnectionParameters(ip, heartbeat_interval=0))
    connection_out = pika.BlockingConnection(pika.ConnectionParameters(ip, heartbeat_interval=0))
    publisher = TaskPublisher(connection_in, connection_out)
    publisher.start()

    return publisher


def start_divider(ip):
    connection_in = pika.BlockingConnection(pika.ConnectionParameters(ip, heartbeat_interval=0))
    connection_out = pika.BlockingConnection(pika.ConnectionParameters(ip, heartbeat_interval=0))
    divider = TaskDivider(connection_in, connection_out)
    divider.start()

    return divider


def start_subscriber(ip):
    connection2 = pika.BlockingConnection(pika.ConnectionParameters(ip, heartbeat_interval=0))
    subscriber = TaskSubscriber(connection2)
    subscriber.start()


def main():
    client_hostname = socket.gethostname()  # CLIENT_HOSTNAME
    ip = socket.gethostbyname(client_hostname)

    publisher = start_publisher(ip)
    divider_proc = Process(target=start_divider, args=[ip])
    divider_proc.start()

    subscriber_proc = [Process(target=start_subscriber, args=[ip]) for __ in range(3)]
    [proc.start() for proc in subscriber_proc]

    for num in range(500):
        hook = publisher.submit_task("fibonacci", [num])
        print(hook())


if __name__ == "__main__":
    main()
