import json
from itertools import cycle
from threading import RLock, Thread
from time import sleep

TASK_SUBMIT_QUEUE_NAME = "task_queue"
TASK_RETURN_QUEUE_NAME = "task_result_queue"


class TaskPublisher(Thread):
    """
    Only one instance of this class should be created during program execution.
    """
    _MAX_ID = 100_000

    def __init__(self, connection):
        Thread.__init__(self)
        output_channel = connection.channel()
        input_channel = connection.channel()

        output_channel.queue_declare(queue=TASK_SUBMIT_QUEUE_NAME, durable=False)
        output_channel.queue_purge(queue=TASK_SUBMIT_QUEUE_NAME)

        input_channel.queue_declare(queue=TASK_RETURN_QUEUE_NAME, durable=False)
        input_channel.queue_purge(queue=TASK_RETURN_QUEUE_NAME)

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
            task_id = result["id"]

            with self.__lock:
                self.__finished_tasks.add(task_id)
                self.__tasks[task_id] = result

        self.__input_channel.basic_consume(process_result, queue=TASK_RETURN_QUEUE_NAME, no_ack=True)
        self.__input_channel.start_consuming()

    def submit_task(self, task_name, task_args):
        """
        Submits a task to be executed on a remote node.
        :param task_name: Name (identifier) of the task to be executed.
        :param task_args: Args of the function that will be called.
        :return: Returns a callable object. When the object is called, it blocks until a result is retrieved.
        The callable returns the result, without exposing the internal mechanisms of the producer-substriber pair.
        """
        task_id = self.__get_next_id()
        task_data = {"task": task_name, "args": task_args, "id": task_id}
        serialized_task = json.dumps(task_data)
        with self.__lock:
            self.__output_channel.basic_publish(exchange="", routing_key=TASK_SUBMIT_QUEUE_NAME,
                                                body=serialized_task)
            self.__tasks[task_id] = None

        def join():
            while True:
                with self.__lock:
                    if task_id in self.__finished_tasks:
                        data = self.__tasks[task_id]
                        del self.__tasks[task_id]
                        self.__finished_tasks.remove(task_id)

                        return data["result"]
                sleep(0.05)

        return join


class TaskSubscriber(Thread):
    """
    Task subscriber aka worker. Ideally, one instance per
    """
    def __init__(self, connection):
        Thread.__init__(self)
        output_channel = connection.channel()
        input_channel = connection.channel()

        output_channel.queue_declare(queue=TASK_SUBMIT_QUEUE_NAME, durable=False)
        output_channel.queue_purge(queue=TASK_SUBMIT_QUEUE_NAME)

        input_channel.queue_declare(queue=TASK_RETURN_QUEUE_NAME, durable=False)
        input_channel.queue_purge(queue=TASK_RETURN_QUEUE_NAME)

        self.__output_channel = output_channel
        self.__input_channel = input_channel

    def run(self):
        def process_request(ch, method, properties, body):
            request = json.loads(body)
            result = self.__execute_task(request["task"], request["args"])
            result_dict = {"id": request["id"], "result": result}
            result_json = json.dumps(result_dict)

            self.__output_channel.basic_publish(exchange="", routing_key=TASK_RETURN_QUEUE_NAME,
                                                body=result_json)

        self.__input_channel.basic_consume(process_request, queue=TASK_SUBMIT_QUEUE_NAME, no_ack=True)
        self.__input_channel.start_consuming()

    @staticmethod
    def __execute_task(task_name, args):
        return args[0] * args[1]
