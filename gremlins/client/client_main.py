import pika
from gremlins.common.task_distribution import TaskPublisher, TaskSubscriber
from multiprocessing import Process
from random import randint


def start_publisher():
    connection_in = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    connection_out = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    publisher = TaskPublisher(connection_in, connection_out)
    publisher.start()

    return publisher


def start_subscriber():
    connection2 = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    subscriber = TaskSubscriber(connection2)
    subscriber.start()


if __name__ == "__main__":
    publisher = start_publisher()
    subscriber_proc = [Process(target=start_subscriber) for __ in range(3)]
    [proc.start() for proc in subscriber_proc]

    while True:
        hook = publisher.submit_task("rotfl", [randint(0, 100), randint(0, 100)])
        print(hook())

