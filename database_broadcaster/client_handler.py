"""
Contains ClientHandler class to handle web sockets.
"""
import json.decoder
from bson.json_util import loads, dumps
import hashlib
import motor.motor_tornado
from bson.son import SON
from database_broadcaster.subscribe_message import SubscribeMessage, InvalidSubscribeMessageError, \
    EventNotFoundError, GeneralSubscribeMessage
from http import HTTPStatus
import tornado.concurrent


class ClientHandler(tornado.websocket.WebSocketHandler):
    """
    Subclasses tornado.websocket.WebSocketHandler.
    Attributes:
         events_subscribed: Dictionary that maintains the events subscribed by the client.
         Here each key is an event ID which is  sha1 fingerprint of subscribed message.
         Subscribed message is represented by SubscribeMessage.
         Each value is a tuple (data hash, SubscribeMessage). In this tuple data hash
         is the sha1 hash of data fetched by path represented by SubscribeMessage.

         db_client:  MotorClient object which bson.son.SON as document_class.
    """
    def __init__(self, application, request, **kwargs):
        """
        :param application: tornado.web.Application object
        :param request: tornado.httputil.HTTPServerRequest object
        :param kwargs: Additional keyword arguments.
        """
        super().__init__(application, request, **kwargs)
        self.db_client = motor.motor_tornado.MotorClient(document_class=SON)
        self.events_subscribed = {}

    def initialize(self, broadcast_queue):
        self.broadcast_queue = broadcast_queue

    def has_subscribed(self, event_id):
        """
        Checks whether client has subscribed to the event or not.
        :param event_id: sha1 fingerprint of SubscribeMessage
        :return: Returns True if the client has subscribed to the event or False otherwise.
        """
        if event_id in self.events_subscribed.keys():
            return True
        else:
            return False

    def open(self):
        """
        Overrides tornado.web.WebSocketHandler.open()
        Add client to the broadcast_queue.
        :return: None
        """
        status = {'status': 'connected'}
        self.write_message(dumps(status))
        print("Web Socket Opened", self.request.remote_ip
              + "-" + self.request.host)
        #self.application.broadcast_queue.add_client(self)
        self.broadcast_queue.add_client(self)

    def check_origin(self, origin):
        return True

    @tornado.gen.coroutine
    def on_message(self, message):
        """
        Overrides tornado.web.WebSocketHandler.on_message(). This coroutine handles basic actions such as
        subscribing, un-subscribing, and un-subscribing all.
        :param message: Received JSON string.
        :return: Returns None
        """
        try:
            s = loads(message)
            print('Incoming Message From ' + self.request.remote_ip + '\n MESSAGE = ' + str(s))
            type_s = s['type']
            if type_s in ('db_subscribe', 'general_subscribe'):
                yield self.subscribe(message, type_s)
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
        """
        Overrides tornado.web.WebSocketHandler.write_error().
        Sends error code and message to the client in JSON format.
        :param status_code: Status Code of error.
        :param kwargs: Additional keyword arguments.
        :return: Returns None
        """
        status = {'status_code': status_code}
        if 'message' in kwargs.keys():
            status['message'] = kwargs['message']
        self.write_message(dumps(status))

    def unsubscribe(self, message):
        """
        Unsubscribe single event.
        :param message: JSON message of format
        {
            "type": "unsubscribe",
            "event_id": "--some event id here--"
        }
        :return: None
        :raises EventNotFoundError: if client hasn't subscribed to the event.
        """
        json_dict = loads(message)
        status = {'status': 'unsubscribed'}
        event_id = json_dict['event_id']
        if event_id not in self.events_subscribed.keys():
            raise EventNotFoundError()
        del self.events_subscribed[event_id]
        self.write_message(dumps(status))

    def unsubscribe_all(self, message):
        """
        Unsubscribe all the events.
        :param message: JSON message of format
        {
            "type": "unsubscribe_all"
        }
        :return:
        """
        status = {}
        self.events_subscribed = {}
        status['status'] = 'unsubscribed all'
        self.write_message(dumps(status))

    @tornado.gen.coroutine
    def subscribe(self, message,type):
        """
         Coroutine to subscribe to an event.
        :param message: JSON message of format
        {
            "type": "db_subscribe",
            "db_name": "--name of database--",
            "collection_name": "--name of collection--",
            "objectId": "--ID of document to be subscribed--",
            "field": "--field on which to register listener--"
        }
        :return: None
        """
        if type == 'db_subscribe':
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

        elif type == 'general_subscribe':
            status = {'status': 'subscribed'}
            sub_msg = GeneralSubscribeMessage.parse_message(message)
            event_id = sub_msg.compute_hash()
            self.events_subscribed[event_id] = ("", sub_msg)
            status['event_id'] = event_id
            self.write_message(dumps(status))

    def on_close(self):
        """
        Overrides tornado.web.WebSocketHandler.on_close().
        Removes client from the broadcasting queue.
        :return: None
        """
        self.broadcast_queue.remove_client(self)
        print("Websocket Closed", self.request.remote_ip
              + "-" + self.request.host)
        super().on_close()

    @tornado.gen.coroutine
    def broadcast_change(self, event):
        """
        Broadcast change to the client.
        :param event:  sha1 fingerprint of subscribed event or (event_id, data).
        :return: None
        """
        if type(event) is tuple:
            status = {
                'event_id': event[0],
                'status': 'data published',
                'data': event[1]
            }
            self.write_message(dumps(status))

        else:
            current_data_hash = self.events_subscribed[event][0]
            sub_message = self.events_subscribed[event][1]
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
                self.events_subscribed[event] = (new_data_hash, sub_message)
                status = {
                    'event_id': event,
                    'status': 'data changed',
                    'data_hash': new_data_hash,
                    'data': document,
                }
                self.write_message(dumps(status))
