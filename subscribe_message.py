import tornado.httpserver
import tornado.web
import tornado.websocket
import json.decoder
from tornado.options import define, options
import tornado.options
import tornado.ioloop as ioloop
from bson.json_util import loads, dumps
from bson.objectid import ObjectId
import hashlib
import tornado.log
import motor.motor_tornado
import tornado.gen
from bson.son import SON

class SubscribeMessage:
    def __init__(self):
        self.db_name = ""
        self.collection_name = ""
        self.objectId = None
        self.field = ""

    @staticmethod
    def parse_message(message):
        sub_message = SubscribeMessage()
        try:
            json_dict = loads(message)
            sub_message.db_name = json_dict['db_name']
            sub_message.collection_name = json_dict['collection_name']

            if 'objectId' in json_dict.keys():
                sub_message.objectId = ObjectId(json_dict['objectId'])
            if 'field' in json_dict.keys():
                sub_message.field = json_dict['field']
            if sub_message.is_valid():
                return sub_message
            else:
                raise InvalidSubscribeMessageError
        except json.decoder.JSONDecodeError as e:
            print("LOG ERROR: " + e.msg)
            raise e
        except KeyError as e:
            print("LOG ERROR: KeyError")
            raise e
        except InvalidSubscribeMessageError as e:
            print("LOG ERROR: " + e.msg)
            raise e

    def is_valid(self):
        if self.db_name is "" or self.collection_name is "":
            return False
        else:
            return True

    def compute_hash(self):
        if self.is_valid():
            s = self.db_name + ":" + self.collection_name + ":" + self.objectId.__str__()
            s += ":" + self.field
            hex_digest = hashlib.sha1(s.encode('utf-8')).hexdigest()
            return hex_digest
        else:
            raise InvalidSubscribeMessageError()

    def get_data_by_data_path(self, connection):
        db = connection[self.db_name]
        collection = db[self.collection_name]
        if not self.field:
            if self.objectId is None:
                cursor = collection.find({})
                return cursor
            else:
                return collection.find_one({'_id': self.objectId})
        else:
            path_parts = self.field.split('.')
            projection = path_parts[0]
            for i in path_parts[1:-1]:
                projection += "." + i
            if not path_parts[-1].isnumeric():
                if len(path_parts) != 1:
                    projection += '.' + path_parts[-1]
                return collection.find_one({'_id': self.objectId}
                                           , {projection: 1})
            else:
                n = int(path_parts[-1], 10)
                return collection.find_one({'_id': self.objectId}
                                           , {'_id': 1, projection: {"$slice": [n, 1]}})


class InvalidSubscribeMessageError(Exception):
    msg = 'Invalid Subscribe Message'
