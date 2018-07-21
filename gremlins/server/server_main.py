from gremlins.common.task_distribution import TaskSubscriber
import pika
from multiprocessing import Process
from gremlins.common.constant_values import CLIENT_HOSTNAME


def start_subscriber():
    connection2 = pika.BlockingConnection(pika.ConnectionParameters(CLIENT_HOSTNAME))
    subscriber = TaskSubscriber(connection2)
    subscriber.start()


if __name__ == "__main__":
    subscriber_proc = []
    try:
        subscriber_proc = [Process(target=start_subscriber) for __ in range(3)]
        [proc.start() for proc in subscriber_proc]
        start_subscriber()
    except KeyboardInterrupt:
        pass
