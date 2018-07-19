import json
from itertools import cycle
from socket import gethostname
from threading import RLock, Thread
from time import sleep, monotonic

TASK_SUBMIT_QUEUE_NAME = "task_queue"
TASK_RETURN_QUEUE_NAME = "task_result_queue"
ACTIVITY_NODE_QUEUE_NAME = "activity_observer_queue"


class NodeActivityObserver(Thread):
    _TIMEOUT = 120
    """
    Class responsible of observing node activity.
    """

    def __init__(self, connection):
        Thread.__init__(self)
        self.__channel = connection.channel()
        self.__channel.queue_declare(queue=ACTIVITY_NODE_QUEUE_NAME, durable=False)
        self.__channel.queue_purge(queue=ACTIVITY_NODE_QUEUE_NAME)
        self.__nodes = dict()
        self.__lock = RLock()

    def run(self):
        def process_result(ch, method, properties, body):
            info = json.loads(body)
            node_name = info["source"]
            with self.__lock:
                self.__nodes[node_name] = monotonic()

        self.__channel.basic_consume(process_result, queue=ACTIVITY_NODE_QUEUE_NAME, no_ack=True)
        self.__channel.start_consuming()

    def remove_if_timed_out(self, node_name):
        with self.__lock:
            now = monotonic()
            node_last_activity = self.__nodes[node_name]

            if now - node_last_activity > self._TIMEOUT:
                del self.__nodes[node_name]
                return True
            else:
                return False


class TaskPublisher(Thread):
    """
    Only one instance of this class should be created during program execution.
    """
    _MAX_ID = 100_000
    _MAX_UNCLAIMED_WAIT_TIME = 15 * 60  # 15 minutes

    def __init__(self, connection, observer):
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
        self.__task_claims = dict()
        self.__task_times = dict()
        self.__activity_observer = observer

    def __get_next_id(self):
        with self.__lock:
            task_id = next(self.__id_source)
            while task_id in self.__tasks:
                task_id += 1
            return task_id

    def run(self):

        def process_result(ch, method, properties, body):
            message = json.loads(body)
            task_id = message["id"]
            message_type = message["type"]
            relevant = (message["time"] == self.__task_times[task_id])

            if relevant:
                if message_type == "result":
                    with self.__lock:
                        self.__finished_tasks.add(task_id)
                        self.__tasks[task_id] = message["result"]
                elif message_type == "claim":
                    with self.__lock:
                        self.__task_claims[task_id] = message["source"]

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
        serialized_task = json.dumps(task_data)
        with self.__lock:
            self.__output_channel.basic_publish(exchange="", routing_key=TASK_SUBMIT_QUEUE_NAME,
                                                body=serialized_task)
            self.__tasks[task_id] = None
            self.__task_times[task_id] = now

        def join():
            while True:
                now = monotonic()
                with self.__lock:
                    not_claimed_for_too_long = (task_id not in self.__task_claims
                                                and now - self.__task_times[task_id] > self._MAX_UNCLAIMED_WAIT_TIME)

                    if task_id in self.__finished_tasks:
                        data = self.__tasks[task_id]
                        del self.__tasks[task_id]
                        del self.__task_claims[task_id]
                        del self.__task_times[task_id]
                        self.__finished_tasks.remove(task_id)

                        return data
                    elif task_id in self.__task_claims or not_claimed_for_too_long:
                        """
                        Task has been claimed suspiciously long ago or it was not claimed for too long.
                        """
                        responsible_node = self.__task_claims[task_id]
                        node_inactive = self.__activity_observer.remove_if_timed_out(responsible_node)

                        if node_inactive or not_claimed_for_too_long:  # retry operation
                            task_data = {"task": task_name, "args": task_args, "id": task_id, "time": now}
                            serialized_task = json.dumps(task_data)

                            if node_inactive:
                                del self.__task_claims[task_id]
                            self.__task_times[task_id] = now
                            self.__output_channel.basic_publish(exchange="", routing_key=TASK_SUBMIT_QUEUE_NAME,
                                                                body=serialized_task)

                sleep(0.05)

        return join


class TaskSubscriber(Thread):
    """
    Task subscriber aka worker. Ideally, one instance per
    """

    def __init__(self, connection):
        Thread.__init__(self)
        Thread.setDaemon(self, True)
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

            initial_response = {"source": gethostname(), "id": request["id"], "type": "claim", "time": request["time"]}
            initial_response_json = json.dumps(initial_response)
            self.__output_channel.basic_publish(exchange="", routing_key=TASK_RETURN_QUEUE_NAME,
                                                body=initial_response_json)

            result = self.__execute_task(request["task"], request["args"])
            result_dict = {"id": request["id"], "result": result, "source": gethostname(), "type": "result",
                           "time": request["time"]}
            result_json = json.dumps(result_dict)

            self.__output_channel.basic_publish(exchange="", routing_key=TASK_RETURN_QUEUE_NAME,
                                                body=result_json)

        self.__input_channel.basic_consume(process_request, queue=TASK_SUBMIT_QUEUE_NAME, no_ack=True)
        self.__input_channel.start_consuming()

    @staticmethod
    def __execute_task(task_name, args):
        return args[0] * args[1]


class NodeActivityReporter(Thread):
    """
    Reports node activity
    """

    def __init__(self, connection):
        Thread.__init__(self)
        self.__channel = connection.channel()
        self.__channel.queue_declare(queue=ACTIVITY_NODE_QUEUE_NAME, durable=False)

    def run(self):
        while True:
            data = {"source": gethostname()}
            json_data = json.dumps(data)
            self.__channel.basic_publish(exchange="", routing_key=ACTIVITY_NODE_QUEUE_NAME,
                                         body=json_data)
            sleep(15)
