import pika
from gremlins.common.task_distribution import TaskPublisher, TaskSubscriber, NodeActivityObserver, NodeActivityReporter


connection1 = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
connection2 = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
connection3 = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
connection4 = pika.BlockingConnection(pika.ConnectionParameters("localhost"))


observer = NodeActivityObserver(connection3)
publisher = TaskPublisher(connection1, observer)
subscriber = TaskSubscriber(connection2)
reporter = NodeActivityReporter(connection4)

publisher.start()
subscriber.start()
observer.start()
reporter.start()

while True:
    from random import randint
    hooks = [x for x in [publisher.submit_task("rotfl", [randint(0, 100), randint(0, 100)]) for __ in range(10)]]
    [print(hook()) for hook in hooks]

channel.close()
