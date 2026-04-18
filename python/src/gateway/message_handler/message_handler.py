import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self._client_token = str(uuid.uuid4())

    def serialize_data_message(self, message):
        fruit, amount = message
        return message_protocol.internal.serialize(
            [self._client_token, fruit, amount]
        )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self._client_token])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return fields[1] if len(fields) >= 2 and fields[0] == self._client_token else []
