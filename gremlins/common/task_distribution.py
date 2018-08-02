import json
from itertools import cycle
from socket import gethostname
from threading import RLock, Thread, Timer
from time import sleep, monotonic
from os import getpid


TASK_SUBMIT_QUEUE_NAME = "task_queue"
TASK_RETURN_QUEUE_NAME = "task_result_queue"
ACTIVITY_NODE_QUEUE_NAME = "activity_observer_queue"
SUB_TASK_SUBMIT_QUEUE_NAME = "sub_task_queue"


class TaskPublisher(Thread):
    """
    Only one instance of this class should be created during program execution.
    """
    _MAX_ID = 100 * 1000

    def __init__(self, connection_in, connection_out):
        Thread.__init__(self)
        output_channel = connection_out.channel()
        input_channel = connection_in.channel()

        output_channel.queue_declare(queue=TASK_SUBMIT_QUEUE_NAME, durable=False)
        output_channel.basic_qos(prefetch_count=2)

        output_channel.queue_purge(queue=TASK_SUBMIT_QUEUE_NAME)

        input_channel.queue_declare(queue=TASK_RETURN_QUEUE_NAME, durable=False)
        input_channel.queue_purge(queue=TASK_RETURN_QUEUE_NAME)

        self.__output_channel = output_channel
        self.__input_channel = input_channel
        self.__tasks = dict()  # task_id -> task_result
        self.__id_source = cycle(range(self._MAX_ID))
        self.__lock = RLock()
        self.__finished_tasks = set()
        self._task_times = dict()

    def __get_next_id(self):
        with self.__lock:
            task_id = next(self.__id_source)
            while task_id in self.__tasks:
                task_id += 1
            return task_id

    def run(self):

        def process_result(ch, method, properties, body):
            message = json.loads(body.decode("utf-8"))
            task_id = message["id"]
            submit_time = message["time"]

            with self.__lock:
                if task_id in self._task_times and self._task_times[task_id] == submit_time and task_id in self.__tasks:
                    """
                    Checks whether task_id actually belongs to the submitted task - there might be an old task
                    with the same id. Submit time is what really matters here.
                    """
                    self.__finished_tasks.add(task_id)
                    self.__tasks[task_id] = message["result"]

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
        now = monotonic()

        task_id = self.__get_next_id()
        task_data = {"task": task_name, "args": task_args, "id": task_id, "time": now}
        serialized_task = json.dumps(task_data).encode("utf-8")
        with self.__lock:
            self.__output_channel.basic_publish(exchange="", routing_key=TASK_SUBMIT_QUEUE_NAME,
                                                body=serialized_task)
            self.__tasks[task_id] = None
            self._task_times[task_id] = now

        task_finished = False
        task_result = None

        def join():
            nonlocal task_finished
            nonlocal task_result

            with self.__lock:
                if task_finished:
                    return task_result
            while True:
                with self.__lock:
                    if task_id in self.__finished_tasks:
                        data = self.__tasks[task_id]
                        del self.__tasks[task_id]
                        del self._task_times[task_id]
                        self.__finished_tasks.remove(task_id)
                        task_finished = True
                        task_result = data

                        return data
                sleep(0.01)

        return join


class TaskSubscriber:
    """
    Task subscriber aka worker. Ideally, one instance per
    """

    def __init__(self, connection):
        output_channel = connection.channel()
        input_channel = connection.channel()

        output_channel.queue_declare(queue=TASK_SUBMIT_QUEUE_NAME, durable=False)
        input_channel.queue_declare(queue=TASK_RETURN_QUEUE_NAME, durable=False)

        self.__output_channel = output_channel
        self.__input_channel = input_channel

    def start(self):
        def process_request(ch, method, properties, body):
            request = json.loads(body.decode('utf-8'))

            result = self.__execute_task(request["task"], request["args"])
            result_dict = {"id": request["id"], "result": result, "source": gethostname(), "type": "result",
                           "time": request["time"]}
            result_json = json.dumps(result_dict).encode("utf-8")

            self.__output_channel.basic_publish(exchange="", routing_key=TASK_RETURN_QUEUE_NAME,
                                                body=result_json)

        self.__input_channel.basic_consume(process_request, queue=TASK_SUBMIT_QUEUE_NAME, no_ack=False)
        self.__input_channel.start_consuming()

    @staticmethod
    def __execute_task(task_name, args):
        return repr(args[0] * args[1]) + "@" + repr(gethostname())


class TaskDivider(Thread):
    """
    Class used to divide a task into multiple subtasks without the need to explicitly synchronize on client side.
    """

    def __init__(self, connection_in, connection_out):
        Thread.__init__(self)

        self.__input_channel = connection_in.channel()
        self.__task_submit_channel = connection_out.channel()

        self.__input_channel.queue_declare(queue=SUB_TASK_SUBMIT_QUEUE_NAME, durable=False)
        self.__task_submit_channel.queue_declare(queue=TASK_SUBMIT_QUEUE_NAME, durable=False)

        self.__task_submit_channel.queue_purge(queue=SUB_TASK_SUBMIT_QUEUE_NAME)

        self.__task_hierarchy = dict()
        self.__awaiting_tasks = dict()  # id -> how many
        self.__task_times = dict()
        self.__task_functions = dict()
        self.__task_results = dict()
        self.__task_types = dict()  # task or sub-task

    def run(self):
        def process_request(ch, method, properties, body):
            request = json.loads(body.decode('utf-8'))
            task_id = request["id"]

            if request["type"] == "divide":
                merge_function = request["function"]
                # TODO
            elif request["type"] == "result":
                result = request["result"]

                self.__task_results[task_id] = result

                has_next_level = True
                current_task_id = task_id
                completed_tasks = []
                while has_next_level:
                    has_next_level = False

                    for task, sub_tasks in self.__task_hierarchy.items():
                        if current_task_id in sub_tasks:
                            self.__awaiting_tasks[current_task_id] -= 1

                            if self.__awaiting_tasks[current_task_id] == 0:
                                current_task_id = task
                                completed_tasks.append(task)
                                has_next_level = True
                                break

                for completed_task in completed_tasks:
                    self.__complete_task(completed_task)

        self.__input_channel.basic_consume(process_request, queue=SUB_TASK_SUBMIT_QUEUE_NAME, no_ack=False)
        self.__input_channel.start_consuming()

    def __complete_task(self, task_id):
        pass

    def __submit_task(self, function_name, arguments, master_task=None):
        pass



