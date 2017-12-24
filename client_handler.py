import json.decoder
from bson.json_util import loads, dumps
import hashlib
import motor.motor_tornado
from bson.son import SON
from subscribe_message import SubscribeMessage, InvalidSubscribeMessageError, \
    EventNotFoundError
from http import HTTPStatus
import tornado.concurrent
from db_wrapper import MotorCollectionWrapper


class ClientHandler(tornado.websocket.WebSocketHandler):

    def __init__(self, application, request, **kwargs):
        super().__init__(application, request, **kwargs)
        self.events_subscribed = {}
        self.db_client = motor.motor_tornado.MotorClient(document_class=SON)

    def has_subscribed(self, event_id):
        if event_id in self.events_subscribed.keys():
            return True
        else:
            return False

    def open(self):
        status = {'status': 'connected'}
        self.write_message(dumps(status))
        print("Web Socket Opened", self.request.remote_ip
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
            elif type_s == 'unsubscribe_all':
                self.unsubscribe_all(message)

        except json.JSONDecodeError:
            self.write_error(HTTPStatus.BAD_REQUEST, message='Invalid Json Format')
        except InvalidSubscribeMessageError as e:
            self.write_error(e.status_code, message=e.msg)
        except KeyError as e:
            self.write_error(HTTPStatus.BAD_REQUEST, message='Please Provide Valid Fields')
        except EventNotFoundError as e:
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
            (hashlib.sha1(str(document).encode('utf-8')).hexdigest(), sub_msg)
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
            status = {
                'event_id': event_id,
                'status': 'data changed',
                'data_hash': new_data_hash,
                'data': document,
            }
            self.write_message(dumps(status))
