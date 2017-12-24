from subscribe_message import SubscribeMessage, InvalidSubscribeMessageError


def generate_subscribe_message_hash(db_name, collection_name, existing_object_id=None, fields=None):
    sub_message = SubscribeMessage(db_name=db_name, collection_name=collection_name, object_id=existing_object_id)

    if not sub_message.is_valid():
        raise InvalidSubscribeMessageError()

    if existing_object_id is None or fields is None:
        return sub_message.compute_hash()

    else:
        event_ids = []
        msg1 = SubscribeMessage(db_name,collection_name)
        event_ids.append(msg1.compute_hash())
        msg2 = SubscribeMessage(db_name,collection_name,existing_object_id)
        event_ids.append(msg2.compute_hash())

        for i in fields:
            sub_message.field = i
            event_id = sub_message.compute_hash()
            event_ids.append(event_id)
        return event_ids