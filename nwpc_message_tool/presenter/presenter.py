import typing

from nwpc_message_tool.message import ProductionEventMessage


class Presenter(object):
    def __init__(self):
        pass

    def process_messages(self, messages: typing.Iterable[ProductionEventMessage]):
        pass