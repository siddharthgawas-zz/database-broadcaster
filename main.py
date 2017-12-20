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
from subscribe_message  import SubscribeMessage, InvalidSubscribeMessageError
define("port", default=8000, type=int, help="Set Port to Run")
define("host", default="0.0.0.0", type=str, help="Set IP to Run")


class ClientHandler(tornado.websocket.WebSocketHandler):

    def __init__(self, application, request, **kwargs):
        super().__init__(application, request, **kwargs)
        self.events_subscribed = {}
        self.db_client = motor.motor_tornado.MotorClient(document_class=SON)

    def open(self):
        status = {'staus': 'connnected'}
        self.write_message(dumps(status))
        print("Websocket Opened", self.request.remote_ip
              + "-" + self.request.host)

    def check_origin(self, origin):
        return True

    @tornado.gen.coroutine
    def on_message(self, message):
        s = loads(message)
        print('Incoming Message From ' + self.request.remote_ip + '\n MESSAGE = ' + str(s))
        type_s = s['type']
        if type_s == 'subscribe':
            self.subscribe(message)
        elif type_s == 'unsubscribe':
            self.unsubscribe(message)
        elif type_s == 'unsubscribe all':
            self.unsubscribe_all(message)

    def unsubscribe(self, message):
        try:
            json_dict = loads(message)
            status = {'status': 'unsubscribed'}
            event_id = json_dict['event_id']
            if event_id not in self.events_subscribed.keys():
                status['status'] = 'event not found'
                self.write_message(dumps(status))
                return
            del self.events_subscribed[event_id]
            self.write_message(dumps(status))
        except json.JSONDecodeError as e:
            status['status'] = 'Improper Json Format'
            self.write_message(dumps(status))
        except KeyError as e:
            status['status'] = 'Invalid Fields'
            self.write_message(dumps(status))

    def unsubscribe_all(self, message):
        status = {}
        self.events_subscribed = {}
        status['status'] = 'unsubscribed all'
        self.write_message(dumps(status))

    @tornado.gen.coroutine
    def subscribe(self, message):
        try:
            status = {'status': "subscribed"}
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
                hashlib.sha1(str(document).encode('utf-8')).hexdigest()
            status['event_id'] = hash_msg
            status['data_hash'] = self.events_subscribed[hash_msg]
            self.write_message(dumps(status))

        except json.JSONDecodeError as e:
            status['status'] = 'Improper JSON  Format'
            self.write_message(dumps(status))
        except InvalidSubscribeMessageError as e:
            status['status'] = '''Invalid Subscribe Message.
                   Ensure db_name and collection_name are not blank'''
            self.write_message(dumps(status))
        except KeyError as e:
            status['status'] = 'Please Provide valid fields'
            self.write_message(dumps(status))

    def on_close(self):
        print("Websocket Closed", self.request.remote_ip
              + "-" + self.request.host)
        super().on_close()


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [(r'/webs', ClientHandler)]
        super().__init__(handlers, debug=True)


if __name__ == '__main__':
    tornado.options.parse_command_line()
    app = Application()
    server = tornado.httpserver.HTTPServer(app)
    server.listen(options.port, options.host)
    ioloop.IOLoop.instance().start()
