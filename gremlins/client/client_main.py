from gremlins.common.task_distribution import TaskSubscriber
import pika
from multiprocessing import Process


def start_subscriber():
    connection2 = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    subscriber = TaskSubscriber(connection2)
    subscriber.start()


if __name__ == "__main__":
    subscriber_proc = [Process(target=start_subscriber) for __ in range(4)]
    [proc.start() for proc in subscriber_proc]