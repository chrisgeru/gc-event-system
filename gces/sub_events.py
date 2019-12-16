import logging
from collections import namedtuple

from .message import Message
from .subscriber import Subscriber

logger = logging.getLogger(__name__)

RegistryEntry = namedtuple('RegistryEntry', ['function', 'type', 'ack_late', 'send_event'])


class EventSubscriber(object):
    def __init__(self, topic_name, name, gcloud_project_id=None):
        self.gcloud_project_id = gcloud_project_id
        self.topic_name = topic_name
        self.name = name
        self._register = {}
        self._obj_desc = "{} {} ({})".format(
            self.__class__.__name__,
            name,
            topic_name
        )

        logger.info('{} Created.'.format(self._obj_desc))

    def start(self):
        self.subscriber = Subscriber(
            self.topic_name, self.name, self._callback, self.gcloud_project_id
        )
        logger.info('{} Started.'.format(self._obj_desc))

    def _callback(self, message):
        """Dispatch registered function or task.

        The function/task signature which will receive the message
        has the following arguments:
            * `data` (string): a json serialized message from PubSub
            * `event_name` (string): The event name from PubSub message
        """
        message = Message(message)
        reg = self._register.get(message.event)
        if reg:
            if not reg.ack_late:
                message.ack()

            function_args = {"data": message.data}
            if reg.send_event:
                function_args.update({"event_name": message.event})
            if reg.type == 'function':
                reg.function(**function_args)
            elif reg.type == 'task':
                reg.function.apply_async(args=(message.data, message.event),)
            logger.info('Event {} processed with "{}" callback. (Send event name: {}).'.format(message.event,
                                                                                               reg.function.__name__,
                                                                                               reg.sent_event))
        else:
            logger.info('Event {} Ignored.'.format(message.event))
        message.ack()

    def register_fsub(self, event_name, function, ack_late=True, send_event=False):
        self._register[event_name] = RegistryEntry(
            function=function,
            ack_late=ack_late,
            send_event=send_event,
            type='function'
        )
        logger.info(
            '{}: Event listener function "{}" registered for {}. (Send event name: {})'.format(
                self._obj_desc,
                function.__name__,
                event_name,
                send_event
            )
        )

    def register_tsub(self, event_name, function, ack_late=True, send_event=False):
        self._register[event_name] = RegistryEntry(
            function=function,
            ack_late=ack_late,
            send_event=send_event,
            type='task'
        )
        logger.info(
            '{}: Event listener task "{}" registered for {}. (Send event name: {}).'.format(
                self._obj_desc,
                function.__name__,
                event_name,
                send_event
            )
        )
