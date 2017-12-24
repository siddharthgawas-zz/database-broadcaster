import threading
from queue import Queue
import tornado.ioloop


class BroadcastingQueue(threading.Thread):
    def __init__(self, size):
        threading.Thread.__init__(self)
        self.q = Queue(size)
        self.size = size
        self.clients = []

    def run(self):
        super().run()
        while True:
            event_id = self.q.get()
            for client in self.clients:
                if client.has_subscribed(event_id):
                    loop = tornado.ioloop.IOLoop.current()
                    loop.spawn_callback(client.broadcast_change, event_id)

    def broadcast_event_id(self, event_id):
        self.q.put(event_id)

    def clear_broadcast_queue(self):
        self.q = Queue(maxsize=self.size)

    def add_client(self, client):
        self.clients.append(client)

    def remove_client(self, client):
        self.clients.remove(client)
