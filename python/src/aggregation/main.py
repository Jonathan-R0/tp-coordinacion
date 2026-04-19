import logging
import os

from common import fruit_item, message_protocol, middleware

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self._by_client = {}
        self._eof_seen = {}

    def _partial_pairs(self, client_token: str):
        bucket = self._by_client.get(client_token, {})
        items = sorted(bucket.values())
        items.reverse()
        chunk = items[:TOP_SIZE]
        return [[fi.fruit, fi.amount] for fi in chunk]

    def _process_data(self, client_token: str, fruit: str, amount: int):
        logging.info("Processing data message")
        bucket = self._by_client.setdefault(client_token, {})
        current = bucket.get(fruit, fruit_item.FruitItem(fruit, 0))
        bucket[fruit] = current + fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, client_token: str):
        logging.info("Received EOF for client token")
        count = self._eof_seen.get(client_token, 0) + 1
        self._eof_seen[client_token] = count
        if count < SUM_AMOUNT:
            return

        partial = self._partial_pairs(client_token)
        payload = message_protocol.internal.serialize([client_token, partial])
        self.output_queue.send(payload)

        self._eof_seen.pop(client_token, None)
        self._by_client.pop(client_token, None)

    def process_messsage(self, message, ack, nack):
        try:
            logging.info("Process message")
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(fields[0], fields[1], int(fields[2]))
            elif len(fields) == 1:
                self._process_eof(fields[0])
            ack()
        except Exception:
            nack()

    def _shutdown(self, signum, frame):
        logging.info("Aggregation received shutdown signal")
        try:
            self.input_exchange.stop_consuming()
        except Exception:
            pass

    def start(self):
        import signal

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            try:
                self.input_exchange.close()
            except Exception:
                pass
            try:
                self.output_queue.close()
            except Exception:
                pass


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
