"""
Main Application.
"""
import json.decoder
from tornado.options import define, options
import tornado.ioloop as ioloop
from database_broadcaster.broadcasting_queue import BroadcastingQueue
from database_broadcaster.client_handler import ClientHandler
import tornado.gen
from database_broadcaster.db_wrapper import MotorCollectionWrapper
from bson.json_util import loads
from http import HTTPStatus

define("port", default=8000, type=int, help="Set Port to Run")
define("host", default="0.0.0.0", type=str, help="Set IP to Run")

class DerivedClientHandler(ClientHandler):

    def __init__(self, application, request, **kwargs):
        super().__init__(application, request, **kwargs)

    @tornado.gen.coroutine
    def on_message(self, message):
        yield super().on_message(message)
        try:
            s = loads(message)
            type = s['type']
            if type == 'insert_one':
                db_name = s['db_name']
                document = s['document']
                collection_name = s['collection_name']
                collection = (self.db_client[db_name])[collection_name]
                collection_wrap = MotorCollectionWrapper(collection, self.application.broadcast_queue)
                collection_wrap.insert_one(document)

            elif type == 'insert_many':
                db_name = s['db_name']
                documents = s['documents']
                collection_name = s['collection_name']
                collection = (self.db_client[db_name])[collection_name]
                collection_wrap = MotorCollectionWrapper(collection, self.application.broadcast_queue)
                collection_wrap.insert_many(documents)

            elif type == 'update_one':
                db_name = s['db_name']
                collection_name = s['collection_name']
                filter = s['filter']
                update = s['update']
                collection = (self.db_client[db_name])[collection_name]
                collection_wrap = MotorCollectionWrapper(collection, self.application.broadcast_queue)
                collection_wrap.update_one(filter, update)

            elif type == 'update_many':
                db_name = s['db_name']
                collection_name = s['collection_name']
                filter = s['filter']
                update = s['update']
                collection = (self.db_client[db_name])[collection_name]
                collection_wrap = MotorCollectionWrapper(collection, self.application.broadcast_queue)
                collection_wrap.update_many(filter, update)
        except json.JSONDecodeError:
            self.write_error(HTTPStatus.BAD_REQUEST, message='Invalid Json Format')
        except KeyError:
            self.write_error(HTTPStatus.BAD_REQUEST,message='Provide Valid Fields')
        finally:
            pass


class RealTimeDbApplication(tornado.web.Application):
    """
    RealTimeDbApplication is a subclass of tornado.web.Application.
    This class is used to create basic real time database application.

    Attributes:
        broadcast_queue: BroadcastingQueue object used to broadcast messages to clients.
    """
    def __init__(self, queue_size=4000, handlers=None,
                 default_host=None, transforms=None, **settings):
        """
        Initializes RealTimeDbApplication.
        :param queue_size: Specifies the size of BroadCastingQueue object.
        :param handlers: List of (url,ClientHandler). More than two ClientHandlers can be
        used which will share broadcast_queue.
        :param default_host:
        :param transforms:
        :param settings: Settings of handlers.
        """
        if handlers is None:
            handlers = [(r'/webs', ClientHandler)]
        self.broadcast_queue = BroadcastingQueue(queue_size)
        super().__init__(handlers, default_host, transforms, **settings)

    def start_broadcast_queue(self):
        """
        This methods is used to start broadcast_queue. broadcast_queue(of type BroadcastingQueue)
        is also an instance of type threading.Thread. Thus this method starts BroadcastingQueue thread.
        :return: Returns None.
        """
        self.broadcast_queue.start()


if __name__ == '__main__':
    tornado.options.parse_command_line()
    app = RealTimeDbApplication(handlers=[(r'/webs', DerivedClientHandler)])
    app.start_broadcast_queue()
    server = tornado.httpserver.HTTPServer(app)
    server.listen(options.port, options.host)
    ioloop.IOLoop.instance().start()
