import pika

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))

channel = connection.channel()

channel.queue_declare(queue="tasks", durable=False)
channel.queue_purge(queue="tasks")

channel.queue_declare(queue="results", durable=False)
channel.queue_purge(queue="results")

channel.close()
