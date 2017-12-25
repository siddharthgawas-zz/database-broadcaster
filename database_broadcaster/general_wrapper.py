"""
General message publisher wrapper class.
"""
from database_broadcaster.subscribe_message import GeneralSubscribeMessage
class GeneralMessagePublisher:
    """
    Used to publish general events to broadcast queues along with the data.
    Attribute:
        queue: BroadcastingQueue object.
    """
    def __init__(self, broadcast_queue):
        self.queue = broadcast_queue

    def publish_message(self,event_path, data):
        """
        Publishes message to the broadcasting queue.
        :param event_path: Event path where data has to be published.
        :param data: Data to be published
        :return: None
        """
        gen_msg = GeneralSubscribeMessage(event_path)
        event_id = gen_msg.compute_hash()
        self.queue.broadcast_event_with_data(event_id, data)
