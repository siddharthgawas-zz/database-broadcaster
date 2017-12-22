import tornado.httpserver
import tornado.web
import tornado.websocket
import json.decoder
from tornado.options import define, options
import tornado.ioloop as ioloop
from bson.json_util import loads, dumps
import hashlib
import motor.motor_tornado
import tornado.gen
from bson.son import SON
from subscribe_message import SubscribeMessage, InvalidSubscribeMessageError, \
    EventNotFoundError, InvalidActionError
from http import HTTPStatus
import tornado.concurrent
from queue import Queue
import threading
from db_wrapper import MotorCollectionWrapper

define("port", default=8000, type=int, help="Set Port to Run")
define("host", default="0.0.0.0", type=str, help="Set IP to Run")


class ClientHandler(tornado.websocket.WebSocketHandler):

    def __init__(self, application, request, **kwargs):
        super().__init__(application, request, **kwargs)
        self.events_subscribed = {}
        self.db_client = motor.motor_tornado.MotorClient(document_class=SON)

    def has_subscribed(self,event_id):
        if event_id in self.events_subscribed.keys():
            return True
        else:
            return False

    def open(self):
        status = {'status': 'connnected'}
        self.write_message(dumps(status))
        print("Websocket Opened", self.request.remote_ip
              + "-" + self.request.host)
        self.application.broadcast_queue.add_client(self)

    def check_origin(self, origin):
        return True

    @tornado.gen.coroutine
    def on_message(self, message):
        try:
            s = loads(message)
            print('Incoming Message From ' + self.request.remote_ip + '\n MESSAGE = ' + str(s))
            type_s = s['type']
            if type_s == 'subscribe':
                yield self.subscribe(message)
            elif type_s == 'unsubscribe':
                self.unsubscribe(message)
            elif type_s == 'unsubscribe all':
                self.unsubscribe_all(message)
            elif type_s == 'insert_one':
                collection = (self.db_client[s['db_name']])[s['collection_name']]
                collection_wrap = MotorCollectionWrapper(collection,
                                                         self.application.broadcast_queue)
                yield collection_wrap.insert_one(s['document'])

            else:
                raise InvalidActionError()

        except json.JSONDecodeError as e:
            self.write_error(HTTPStatus.BAD_REQUEST, message='Invalid Json Format')
        except InvalidSubscribeMessageError as e:
            self.write_error(e.status_code, message=e.msg)
        except KeyError as e:
            self.write_error(HTTPStatus.BAD_REQUEST, message='Please Provide Valid Fields')
        except EventNotFoundError as e:
            self.write_error(e.status_code, message=e.msg)
        except InvalidActionError as e:
            self.write_error(e.status_code, message=e.msg)

    def write_error(self, status_code, **kwargs):
        status = {'status_code': status_code}
        if 'message' in kwargs.keys():
            status['message'] = kwargs['message']
        self.write_message(dumps(status))

    def unsubscribe(self, message):
        json_dict = loads(message)
        status = {'status': 'unsubscribed'}
        event_id = json_dict['event_id']
        if event_id not in self.events_subscribed.keys():
            raise EventNotFoundError()
        del self.events_subscribed[event_id]
        self.write_message(dumps(status))

    def unsubscribe_all(self, message):
        status = {}
        self.events_subscribed = {}
        status['status'] = 'unsubscribed all'
        self.write_message(dumps(status))

    @tornado.gen.coroutine
    def subscribe(self, message):
        status = {'status': 'subscribed'}
        sub_msg = SubscribeMessage.parse_message(message)
        hash_msg = sub_msg.compute_hash()
        result = sub_msg.get_data_by_data_path(self.db_client)
        if type(result) is motor.motor_tornado.MotorCursor:
            document = []
            while (yield result.fetch_next):
                doc = result.next_object()
                document.append(doc)
        else:
            document = yield sub_msg.get_data_by_data_path(self.db_client)
        status['data'] = document

        self.events_subscribed[hash_msg] = \
            (hashlib.sha1(str(document).encode('utf-8')).hexdigest(),sub_msg)
        status['event_id'] = hash_msg
        status['data_hash'] = self.events_subscribed[hash_msg][0]
        self.write_message(dumps(status))

    def on_close(self):
        self.application.broadcast_queue.remove_client(self)
        print("Websocket Closed", self.request.remote_ip
              + "-" + self.request.host)
        super().on_close()

    @tornado.gen.coroutine
    def broadcast_change(self, event_id):
        current_data_hash = self.events_subscribed[event_id][0]
        sub_message = self.events_subscribed[event_id][1]

        result = sub_message.get_data_by_data_path(self.db_client)
        if type(result) is motor.motor_tornado.MotorCursor:
            document = []
            while (yield result.fetch_next):
                doc = result.next_object()
                document.append(doc)
        else:
            document = yield sub_message.get_data_by_data_path(self.db_client)

        new_data_hash = hashlib.sha1(str(document).encode('utf-8')).hexdigest()
        if new_data_hash != current_data_hash:
            self.events_subscribed[event_id] = (new_data_hash, sub_message)
            status ={
                'event_id': event_id,
                'status': 'data changed',
                'data_hash': new_data_hash,
                'data': document,
            }
            self.write_message(dumps(status))

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
            client = self.clients[0]
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


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [(r'/webs', ClientHandler)]
        super().__init__(handlers, debug=True)

        # provide options for queue size
        self.broadcast_queue = BroadcastingQueue(4000)
        self.broadcast_queue.start()


if __name__ == '__main__':
    tornado.options.parse_command_line()
    app = Application()
    server = tornado.httpserver.HTTPServer(app)
    server.listen(options.port, options.host)
    ioloop.IOLoop.instance().start()
