import tornado.httpserver
import tornado.web
import tornado.websocket
import json.decoder
from tornado.options import define, options
import tornado.ioloop as ioloop
import tornado.concurrent
from broadcasting_queue import BroadcastingQueue
from client_handler import ClientHandler
import tornado.gen
from db_wrapper import MotorCollectionWrapper
from bson.json_util import dumps,loads
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
                collection_wrap.update_one(filter,update)
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
    def __init__(self, queue_size=4000, handlers=None,
                 default_host=None, transforms=None, **settings):
        if handlers is None:
            handlers = [(r'/webs', ClientHandler)]
        self.broadcast_queue = BroadcastingQueue(queue_size)
        super().__init__(handlers, default_host, transforms, **settings)

    def start_broadcast_queue(self):
        self.broadcast_queue.start()


if __name__ == '__main__':
    tornado.options.parse_command_line()
    app = RealTimeDbApplication(handlers=[(r'/webs', DerivedClientHandler)])
    app.start_broadcast_queue()
    server = tornado.httpserver.HTTPServer(app)
    server.listen(options.port, options.host)
    ioloop.IOLoop.instance().start()
