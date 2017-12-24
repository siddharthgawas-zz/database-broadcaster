import sub_msg_gen as sg
import tornado.gen

class MotorCollectionWrapper:

    def __init__(self, collection, broadcast_queue):
        self.collection = collection
        self.queue = broadcast_queue

    @tornado.gen.coroutine
    def insert_one(self, document, *args, **kwargs):
        db_name = self.collection.database.name
        collection_name = self.collection.name
        event_id = sg.generate_subscribe_message_hash(db_name, collection_name)
        future = self.collection.insert_one(document, *args, **kwargs)
        result = yield future
        if result.inserted_id is not None:
            self.queue.broadcast_event_id(event_id)
        return result

    @tornado.gen.coroutine
    def insert_many(self, documents, *args, **kwargs):
        db_name = self.collection.database.name
        collection_name = self.collection.name
        event_id = sg.generate_subscribe_message_hash(db_name, collection_name)
        future = self.collection.insert_many(documents, *args, **kwargs)
        result = yield future

        if len(result.inserted_ids) != 0:
            self.queue.broadcast_event_id(event_id)
        return result

    #not fully tested
    @tornado.gen.coroutine
    def update_one(self, filter,update,*args,**kwargs):
        db_name = self.collection.database.name
        collection_name  = self.collection.name
        future = self.collection.update_one(filter,update,*args,**kwargs)
        update_result = yield future

        #need to optimize this method
        if update_result.modified_count > 0:
            document_id = yield self.collection.find_one(filter, {"_id": 1})
            id = str(document_id['_id'])
            fields = []
            for g in update.keys():
                for f in update[g]:
                    fields.append(f)
            event_ids = sg.generate_subscribe_message_hash(db_name, collection_name, id, fields)
            for event_id in event_ids:
                self.queue.broadcast_event_id(event_id)
        return update_result

    #complete testing required
    @tornado.gen.coroutine
    def update_many(self,filter,update,*args, **kwargs):
        db_name = self.collection.database.name
        collection_name = self.collection.name
        future = self.collection.update_many(filter, update, *args, **kwargs)
        update_result = yield future

        if update_result.modified_count > 0:
            document_ids = []
            cursor = self.collection.find(filter, {'_id': 1})
            while (yield cursor.fetch_next):
                doc = cursor.next_object()
                id = str(doc['_id'])
                document_ids.append(id)
            del id

            for id in document_ids:
                fields = []
                for g in update.keys():
                    for f in update[g]:
                        fields.append(f)
                event_ids = sg.generate_subscribe_message_hash(db_name, collection_name, id, fields)
                for event_id in event_ids:
                    self.queue.broadcast_event_id(event_id)
        return update_result







