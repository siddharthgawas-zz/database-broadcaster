import sub_msg_gen as sg
import tornado.gen

class MotorCollectionWrapper:
    def __init__(self, collection,broadcast_queue):
        self.collection = collection
        self.queue = broadcast_queue

    @tornado.gen.coroutine
    def insert_one(self, document, *args, **kwargs):
        db_name = self.collection.databse.name
        db_name = db_name.split('.')[0]
        collection_name = self.collection.name
        event_id = sg.generate_subscribe_message_hash(db_name, collection_name)
        future = self.collection.insert_one(document, *args, **kwargs)
        result = yield future
        if result.inserted_id is not None:
            self.queue.broadcast_event_id(event_id)
        return result