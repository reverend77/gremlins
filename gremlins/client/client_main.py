import pika
from gremlins.common.task_distribution import TaskPublisher, TaskSubscriber


connection1 = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
connection2 = pika.BlockingConnection(pika.ConnectionParameters("localhost"))

publisher = TaskPublisher(connection1)
subscriber = TaskSubscriber(connection2)

publisher.start()
subscriber.start()

while True:
    from random import randint
    hook = publisher.submit_task("rotfl", [randint(0,100), randint(0, 100)])
    print(hook())

channel.close()
