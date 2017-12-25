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
    that contains event fingerprints.
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

    def run(self):
        """
        Contains infinite loop that broadcasts events to clients that has subscribed to that
        event.
        :return: None
        """
        super().run()
        while True:
            event_id = self.q.get()
            for client in self.clients:
                if client.has_subscribed(event_id):
                    loop = tornado.ioloop.IOLoop.current()
                    loop.spawn_callback(client.broadcast_change, event_id)

    def broadcast_event_id(self, event_id):
        """
        Puts an event ID in q to be broadcast to the subscribed clients.
        :param event_id: Event ID to be broadcast.
        :return: None
        """
        self.q.put(event_id)

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

    def remove_client(self, client):
        """
        Removes client from the clients list.
        :param client: Object of type ClientHandler.
        :return: None.
        """
        self.clients.remove(client)
