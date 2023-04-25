import collections

_incoming_messages = collections.deque([], 20)
_outgoing_messages = collections.deque([], 20)

# TODO: Add a message history class to store the last 20 messages
class MessageHistory:

    @staticmethod
    def add_incoming_message(uuid, message):
        _incoming_messages.append((uuid, message))

    @staticmethod
    def add_outgoing_message(uuid, message):
        _outgoing_messages.append((uuid, message))

    @staticmethod
    def get_incoming_messages():
        return list(_incoming_messages)

    @staticmethod
    def get_outgoing_messages():
        return list(_outgoing_messages)

    @staticmethod
    def get_incoming_message(uuid):
        for message in _incoming_messages:
            if message[0] == uuid:
                return message
        return None

    @staticmethod
    def get_outgoing_message(uuid):
        for message in _outgoing_messages:
            if message[0] == uuid:
                return message
        return None
