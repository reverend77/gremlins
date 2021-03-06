import socket
import pika
from time import monotonic
from gremlins.common.utils import Worker
from gremlins.common.task_distribution import TaskPublisher, TaskSubscriber, TaskDivider


def start_publisher(ip):
    connection_in = pika.BlockingConnection(pika.ConnectionParameters(ip, heartbeat=0))
    connection_out = pika.BlockingConnection(pika.ConnectionParameters(ip, heartbeat=0))
    publisher = TaskPublisher(connection_in, connection_out)
    publisher.start()

    return publisher


def start_divider(ip):
    connection_in = pika.BlockingConnection(pika.ConnectionParameters(ip, heartbeat=0))
    connection_out = pika.BlockingConnection(pika.ConnectionParameters(ip, heartbeat=0))
    divider = TaskDivider(connection_in, connection_out)
    divider.start()

    return divider


def start_subscriber(ip):
    connection2 = pika.BlockingConnection(pika.ConnectionParameters(ip, heartbeat=0))
    subscriber = TaskSubscriber(connection2)
    subscriber.start()


def main():
    client_hostname = socket.gethostname()  # CLIENT_HOSTNAME
    ip = socket.gethostbyname(client_hostname)

    publisher = start_publisher(ip)
    divider_proc = Worker(target=start_divider, args=[ip])
    divider_proc.start()

    subscriber_proc = [Worker(target=start_subscriber, args=[ip]) for __ in range(3)]
    [proc.start() for proc in subscriber_proc]

    for num in range(500):
        start = monotonic()
        hook = publisher.submit_task("fibonacci", [num])
        result = hook()
        end = monotonic()
        print("Time: {}|Index: {}| Value: {}".format(int(end-start), num, result))


if __name__ == "__main__":
    main()
