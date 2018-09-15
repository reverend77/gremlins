from gremlins.common.task_distribution import TaskSubscriber
import pika
from gremlins.common.constant_values import CLIENT_HOSTNAME
from gremlins.common.utils import Worker


def start_subscriber():
    connection2 = pika.BlockingConnection(pika.ConnectionParameters(CLIENT_HOSTNAME, heartbeat=0))
    subscriber = TaskSubscriber(connection2)
    subscriber.start()


def main():
    subscriber_proc = []
    try:
        subscriber_proc = [Worker(target=start_subscriber) for __ in range(3)]
        [proc.start() for proc in subscriber_proc]
        start_subscriber()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
