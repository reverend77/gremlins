import json
from itertools import cycle
from socket import gethostname
from threading import RLock, Thread, Timer
from time import sleep, monotonic
from os import getpid


TASK_SUBMIT_QUEUE_NAME = "task_queue"
TASK_RETURN_QUEUE_NAME = "task_result_queue"
ACTIVITY_NODE_QUEUE_NAME = "activity_observer_queue"


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


HEARTBEAT_QUEUE_NAME = "heartbeat_queue"


class NodeHeartbeatListener(Thread):
    HEARTBEAT_TIMEOUT = 60

    def __init__(self, connection):
        Thread.__init__(self)
        self.__channel = connection.channel()

        self.__channel.queue_declare(queue=HEARTBEAT_QUEUE_NAME, durable=False)
        self.__channel.basic_qos(prefetch_count=2)
        self.__channel.queue_purge(queue=HEARTBEAT_QUEUE_NAME)

        self.__lock = RLock()
        self.__timers = dict()

    def run(self):

        def process_request(ch, method, properties, body):
            with self.__lock:
                heartbeat = json.loads(body.decode('utf-8'))

                node_name = heartbeat["node"]
                if node_name in self.__timers:
                    self.__timers[node_name].cancel()
                    del self.__timers[node_name]

                timer = Timer(self.HEARTBEAT_TIMEOUT, lambda: self.timeout_reached(node_name))
                self.__timers[node_name] = timer
                timer.start()

        self.__channel.basic_consume(process_request, queue=HEARTBEAT_QUEUE_NAME, no_ack=False)
        self.__channel.start_consuming()

    def timeout_reached(self, node):
        with self.__lock:
            pass




class NodeHeartbeatProducer(Thread):
    HEARTBEAT_PERIOD = 15

    def __init__(self, connection):
        Thread.__init__(self)
        self.__channel = connection.channel()

        self.__channel.queue_declare(queue=TASK_SUBMIT_QUEUE_NAME, durable=False)

    def run(self):
        while True:
            heartbeat = json.dumps({"node": "NODE={};PID={}".format(gethostname(), getpid())})
            self.__channel.basic_publish(exchange="", routing_key=HEARTBEAT_QUEUE_NAME, body=heartbeat)
            sleep(self.HEARTBEAT_PERIOD)


