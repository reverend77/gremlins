import json
from itertools import cycle
from socket import gethostname
from threading import RLock, Thread
from time import sleep, monotonic

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

    def __get_next_id(self):
        """
        ID combines time and an generated identifier.
        :return: string with new, unique task id
        """
        with self.__lock:
            task_id = next(self.__id_source)
            return "{}@{}".format(task_id, monotonic())

    def run(self):

        def process_result(ch, method, properties, body):
            message = json.loads(body.decode("utf-8"))
            task_id = message["id"]

            with self.__lock:
                if task_id in self.__tasks:
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

        task_id = self.__get_next_id()
        task_data = {"task": task_name, "args": task_args, "id": task_id, "is_root_task": True}
        serialized_task = json.dumps(task_data).encode("utf-8")
        with self.__lock:
            self.__output_channel.basic_publish(exchange="", routing_key=TASK_SUBMIT_QUEUE_NAME,
                                                body=serialized_task)
            self.__tasks[task_id] = None

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
                        self.__finished_tasks.remove(task_id)
                        task_finished = True
                        task_result = data

                        return data
                sleep(0.01)

        return join


class TaskDivisionExpectedException(Exception):

    def __init__(self, merge_function, sub_tasks):
        Exception.__init__(self)
        self.merge_function = merge_function
        self.sub_tasks = sub_tasks


class TaskSubscriber:
    """
    Task subscriber aka worker. Ideally, one instance per
    """

    def __init__(self, connection):
        output_channel = connection.channel()
        input_channel = connection.channel()

        output_channel.queue_declare(queue=TASK_SUBMIT_QUEUE_NAME, durable=False)
        output_channel.queue_declare(queue=SUB_TASK_SUBMIT_QUEUE_NAME, durable=False)
        input_channel.queue_declare(queue=TASK_RETURN_QUEUE_NAME, durable=False)

        self.__output_channel = output_channel
        self.__input_channel = input_channel

    def start(self):
        def process_request(ch, method, properties, body):
            request = json.loads(body.decode('utf-8'))
            task_id = request["id"]

            is_root_task = request["is_root_task"]

            try:
                result = self.__execute_task(request["task"], request["args"])
                result_dict = {"id": task_id, "result": result, "source": gethostname(), "type": "result"}

                result_json = json.dumps(result_dict).encode("utf-8")

                self.__output_channel.basic_publish(exchange="",
                                                    routing_key=
                                                    TASK_RETURN_QUEUE_NAME if is_root_task
                                                    else SUB_TASK_SUBMIT_QUEUE_NAME,
                                                    body=result_json)
            except TaskDivisionExpectedException as details:
                new_request = {"id": task_id, "source": gethostname(), "type": "division",
                               "sub_tasks": details.sub_tasks, "task": details.merge_function,
                               "is_root_task": is_root_task}

                new_request_json = json.dumps(new_request).encode("utf-8")
                self.__output_channel.basic_publish(exchange="",
                                                    routing_key=SUB_TASK_SUBMIT_QUEUE_NAME,
                                                    body=new_request_json)

        self.__input_channel.basic_consume(process_request, queue=TASK_SUBMIT_QUEUE_NAME, no_ack=False)
        self.__input_channel.start_consuming()

    @staticmethod
    def __execute_task(task_name, args):
        if task_name == "sum":
            return sum(args)
        elif task_name == "fibonacci":
            num = args[0]
            if num <= 1:
                return 1
            else:
                sub_task_1 = {"task": "fibonacci", "args": [num - 1]}
                sub_task_2 = {"task": "fibonacci", "args": [num - 2]}
                raise TaskDivisionExpectedException("sum", [sub_task_1, sub_task_2])
        else:
            return repr(args[0] * args[1]) + "@" + repr(gethostname())


class TaskDivider(Thread):
    """
    Class used to divide a task into multiple subtasks without the need to explicitly synchronize on client side.
    """

    _MAX_ID = 100_000

    def __init__(self, connection_in, connection_out):
        Thread.__init__(self)

        self.__input_channel = connection_in.channel()
        self.__task_submit_channel = connection_out.channel()

        self.__input_channel.queue_declare(queue=SUB_TASK_SUBMIT_QUEUE_NAME, durable=False)
        self.__task_submit_channel.queue_declare(queue=TASK_SUBMIT_QUEUE_NAME, durable=False)

        self.__task_submit_channel.queue_purge(queue=SUB_TASK_SUBMIT_QUEUE_NAME)

        self.__task_hierarchy = dict()
        self.__awaiting_tasks = dict()  # id -> how many
        self.__task_functions = dict()
        self.__task_results = dict()
        self.__task_parent = dict()

        self.__root_tasks = set()

        self.__id_source = cycle(range(self._MAX_ID))

    def __get_next_id(self, parent_id):
        """
        ID combines time and an generated identifier.
        :return: string with new, unique task id
        """
        task_id = next(self.__id_source)
        return "{}@{}@{}".format(parent_id, task_id, monotonic())

    def run(self):
        def process_request(ch, method, properties, body):
            request = json.loads(body.decode('utf-8'))
            task_id = request["id"]

            if request["type"] == "division":

                if request["is_root_task"]:
                    self.__root_tasks.add(task_id)

                merge_function = request["task"]
                sub_tasks = request["sub_tasks"]

                self.__awaiting_tasks[task_id] = 0
                self.__task_hierarchy[task_id] = []
                self.__task_functions[task_id] = merge_function

                for sub_task in sub_tasks:
                    function_name = sub_task["task"]
                    function_args = sub_task["args"]

                    sub_task_id = self.__get_next_id(task_id)
                    self.__awaiting_tasks[task_id] += 1
                    self.__task_hierarchy[task_id].append(sub_task_id)
                    self.__task_parent[sub_task_id] = task_id
                    self.__submit_task(function_name, function_args, sub_task_id)

            elif request["type"] == "result":
                result = request["result"]
                parent_task = self.__task_parent[task_id]
                self.__task_results[task_id] = result

                self.__awaiting_tasks[parent_task] -= 1

                if self.__awaiting_tasks[parent_task] == 0:
                    self.__merge_divided_task(parent_task)

        self.__input_channel.basic_consume(process_request, queue=SUB_TASK_SUBMIT_QUEUE_NAME, no_ack=False)
        self.__input_channel.start_consuming()

    def __merge_divided_task(self, task_id):

        args = []
        for sub_task_id in self.__task_hierarchy[task_id]:
            args.append(self.__task_results[sub_task_id])
            del self.__task_results[sub_task_id]
            del self.__task_parent[sub_task_id]

        if task_id in self.__root_tasks:
            self.__root_tasks.remove(task_id)
            is_root = True
        else:
            is_root = False

        function_to_call = self.__task_functions[task_id]

        del self.__task_functions[task_id]
        del self.__task_hierarchy[task_id]
        del self.__awaiting_tasks[task_id]

        self.__submit_task(function_to_call, args, task_id, is_root_task=is_root)

    def __submit_task(self, function_name, arguments, task_id, is_root_task=False):
        task_data = {"task": function_name, "args": arguments, "id": task_id, "is_root_task": is_root_task}

        serialized_task = json.dumps(task_data).encode("utf-8")

        self.__task_submit_channel.basic_publish(exchange="", routing_key=TASK_SUBMIT_QUEUE_NAME,
                                                 body=serialized_task)
