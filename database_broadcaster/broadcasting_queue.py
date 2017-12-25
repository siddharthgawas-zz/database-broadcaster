"""
Contains class BroadcastingQueue used for broadcasting events.
"""
import threading
from queue import Queue
import tornado.ioloop

class BroadcastingQueue(threading.Thread):
    """
    BroadcastingQueue subclasses threading.Thread. Objects  of this class
    basically sends event messages to the the clients. It maintains an queue
    that contains event fingerprints. BroadcastingQueue is a self starting queue. Thread to
    process queue will be started automatically when a client is added. Thread is destroyed
    when queue has no clients.
    Attributes:
        q: First in First Queue object. Contains fingerprints of events called event IDs.
        size: Size of the queue q.
        clients: List of all the clients. Each element is of type ClientHandler.
    """
    def __init__(self, size):
        """
        Initializes BroadCastingQueue object.
        :param size: Size of the Queue q.
        """
        threading.Thread.__init__(self)
        self.q = Queue(size)
        self.size = size
        self.clients = []
        self.loop_flag = True

    def run(self):
        """
        Contains infinite loop that broadcasts events to clients that has subscribed to that
        event.
        :return: None
        """
        super().run()
        while self.loop_flag:
            event = self.q.get()

            if event is None:
                continue
            elif type(event) is tuple:
                for client in self.clients:
                    if client.has_subscribed(event[0]):
                        loop = tornado.ioloop.IOLoop.current()
                        loop.spawn_callback(client.broadcast_change, event)
            else:
                for client in self.clients:
                    if client.has_subscribed(event):
                        loop = tornado.ioloop.IOLoop.current()
                        loop.spawn_callback(client.broadcast_change, event)

    def broadcast_event_id(self, event_id):
        """
        Puts an event ID in q to be broadcast to the subscribed clients.
        :param event_id: Event ID to be broadcast.
        :return: None
        """
        self.q.put(event_id)

    def broadcast_event_with_data(self, event_id, data):
        """
        Puts an event ID and data to be broadcast as tuple(event_id, data)
        in q.
        :param event_id: sha1 fingerprint of event.
        :param data: Data to be published.
        :return: None
        """
        item = (event_id, data)
        self.q.put(item)

    def clear_broadcast_queue(self):
        """
        Clears broadcast queue by Re-initializing q.
        :return: None
        """
        self.q = Queue(maxsize=self.size)

    def add_client(self, client):
        """
        Adds client to the clients list.
        :param client: Object of type ClientHandler.
        :return: None
        """
        self.clients.append(client)
        if len(self.clients) == 1:
            self.start()

    def remove_client(self, client):
        """
        Removes client from the clients list.
        :param client: Object of type ClientHandler.
        :return: None.
        """
        self.clients.remove(client)
        if len(self.clients) == 0:
            self.stop()

    def stop(self):
        """
        Stop this thread.
        :return: None
        """
        lock = threading.Lock()
        lock.acquire()
        self.loop_flag = False
        self.q.put(None)
        lock.release()
