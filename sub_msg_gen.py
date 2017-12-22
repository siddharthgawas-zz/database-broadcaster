from subscribe_message import SubscribeMessage, InvalidSubscribeMessageError


def generate_subscribe_message_hash(db_name, collection_name, existing_object_id=None, fields=None):
    sub_message = SubscribeMessage(db_name=db_name, collection_name=collection_name, object_id=existing_object_id)

    if not sub_message.is_valid():
        raise InvalidSubscribeMessageError()

    if existing_object_id is None or fields is None:
        return sub_message.compute_hash()
    else:
        pass
