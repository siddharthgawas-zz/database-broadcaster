import tornado.httpserver
import tornado.web
import tornado.websocket
import json.decoder
import tornado.options
from bson.json_util import loads, dumps
from bson.objectid import ObjectId
import hashlib
import tornado.log
import tornado.gen
import tornado.concurrent


class SubscribeMessage:
    """
    This class represents the part of database to be subscribed.
    Attributes:
        db_name: Mandatory attribute that takes the name of the database.
        collection_name: Mandatory attribute that takes name of the collection.
        objectId: Optional object ID of document to be subscribed.
        field: Optional field of document to be subscribed.
    """
    def __init__(self):
        """
        Initializes SubscribeMessage object.
        """
        self.db_name = ""
        self.collection_name = ""
        self.objectId = None
        self.field = ""

    def __init__(self, db_name="", collection_name="", object_id=None, field=""):
        """
        Initializes SubscribeMessage object.
        """
        self.db_name = db_name
        self.collection_name = collection_name
        self.objectId = object_id
        self.field = field

    @staticmethod
    def parse_message(message):
        """
        Static methods that creates SubscribeMessage object based the JSON subscribe message.
        :param message: JSON subscribe message of the following format.
        {
            "type": "subscribe",
            "db_name": "--name of database--",
            "collection_name": "--name of collection--",
            "objectId": "--optional ID of document to be subscribed--",
            "field": "--optional field on which to register listener--"
        }
        :return: Returns SubscribeMessage object.
        :raises KeyError: Raised if the JSON subscribe message contains invalid fields.
        :raises InvalidSubscribeMessageError: Raised if the SubscribeMessage is invalid.
        """
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
        except KeyError as e:
            print("LOG ERROR: KeyError")
            raise
        except InvalidSubscribeMessageError as e:
            print("LOG ERROR: " + e.msg)
            raise

    def is_valid(self):
        """
        Checks if the SubscribeMessage is valid or not. SubscribeMessage is valid
        if db_name and collection_name is not empty, invalid otherwise.
        :return: Returns True if db_name and collection_name are not empty. False otherwise.
        """
        if self.db_name is "" or self.collection_name is "":
            return False
        else:
            return True

    def compute_hash(self):
        """
        Computes sha1 fingerprint of the subscribe message which is called Event ID.
        :return: Returns sha1 fingerprint of SubscribeMessage.
        :raises InvalidSubscribeMessageError: Raised if the SubscribeMessage is invalid.
        """
        if self.is_valid():
            s = self.db_name + ":" + self.collection_name + ":" + self.objectId.__str__()
            s += ":" + self.field
            hex_digest = hashlib.sha1(s.encode('utf-8')).hexdigest()
            return hex_digest
        else:
            raise InvalidSubscribeMessageError()

    def get_data_by_data_path(self, connection):
        """
        Fetches the data pointed by this subscribe message.
        :param connection: MotorClient object.
        :return: Returns Future if the subscribe message points to specific document. Else returns
        MotorCursor if subscribe message is on whole collection.
        """
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
    """
    Error that can be raised if the SubscribeMessage is invalid.
    Status Code: 1001
    Message: Invalid Subscribe Message
    Attributes:
        status_code: Status code of this error.
    """
    status_code = 1001
    msg = 'Invalid Subscribe Message'


class EventNotFoundError(Exception):
    """
    Error that can be raised if the event is not subscribed by client.
    Status Code: 1002
    Message: Event Not Found. Please ensure that event is already registered
    Attributes:
        status_code: Status code of this error.
    """
    status_code = 1002
    msg = 'Event Not Found. Please ensure that event is already registered'


class InvalidActionError(Exception):
    """
    Error that can be raised if the action requested by client is invalid action.
    Status Code: 1003
    Message: Invalid Action
    Attributes:
        status_code: Status code of this error.
    """
    status_code = 1003
    msg = 'Invalid Action'
