import json
from itertools import cycle
from threading import RLock, Thread
from time import sleep

__TASK_SUBMIT_QUEUE_NAME = "task_queue"
__TASK_RETURN_QUEUE_NAME = "task_result_queue"
"""
Only one instance of this class should be created during program execution.
"""


class TaskPublisher(Thread):
    _MAX_ID = 100_000

    def __init__(self, connection):
        output_channel = connection.channel()
        input_channel = connection.channel()

        output_channel.queue_declare(queue=__name__.__TASK_SUBMIT_QUEUE_NAME, durable=False)
        output_channel.queue_purge(queue=__name__.__TASK_SUBMIT_QUEUE_NAME)

        input_channel.queue_declare(queue=__name__.__TASK_RETURN_QUEUE_NAME, durable=False)
        input_channel.queue_purge(queue=__name__.__TASK_RETURN_QUEUE_NAME)

        self.__output_channel = output_channel
        self.__input_channel = input_channel
        self.__tasks = dict()  # task_id -> task_result
        self.__id_source = cycle(range(self._MAX_ID))
        self.__lock = RLock()
        self.__finished_tasks = set()

    def __get_next_id(self):
        with self.__lock:
            task_id = next(self.__id_source)
            while task_id in self.__tasks:
                task_id += 1
            return task_id

    def run(self):

        def process_result(ch, method, properties, body):
            result = json.loads(body)
            task_id = body["id"]

            with self.__lock:
                self.__finished_tasks.add(task_id)
                self.__tasks[task_id] = result

        self.__input_channel.basic_consume(process_result, queue=__name__.__TASK_RETURN_QUEUE_NAME, no_ack=True)

        self.__input_channel.start_consuming()

    def submit_task(self, task_name, task_args):
        task_id = self.__get_next_id()
        task_data = {"task": task_name, "args": task_args, "id": task_id}
        serialized_task = json.dumps(task_data)
        with self.__lock:
            self.__output_channel.basic_publish(exchange="", routing_key=__name__.__TASK_SUBMIT_QUEUE_NAME,
                                                body=serialized_task)
            self.__tasks[task_id] = None

        def join():
            while True:
                with self.__lock:
                    if task_id in self.__finished_tasks:
                        data = self.__tasks[task_id]
                        del self.__tasks[task_id]
                        self.__finished_tasks.remove(task_id)

                        return data
                sleep(0.05)

        return join


class TaskSubscriber(Thread):
    pass
