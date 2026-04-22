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
        self._seen_data = {}

    def _partial_pairs(self, client_token: str):
        bucket = self._by_client.get(client_token, {})
        items = sorted(bucket.values())
        items.reverse()
        chunk = items[:TOP_SIZE]
        return [[fi.fruit, fi.amount] for fi in chunk]

    def _process_data(self, client_token: str, sum_id: int, fruit: str, amount: int):
        logging.info("Processing data message")
        dedupe_key = (sum_id, fruit)
        seen = self._seen_data.setdefault(client_token, set())
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)

        bucket = self._by_client.setdefault(client_token, {})
        current = bucket.get(fruit, fruit_item.FruitItem(fruit, 0))
        bucket[fruit] = current + fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, client_token: str, sum_id: int):
        logging.info("Received EOF for client token")
        seen = self._eof_seen.setdefault(client_token, set())
        seen.add(sum_id)
        if len(seen) < SUM_AMOUNT:
            return

        partial = self._partial_pairs(client_token)
        payload = message_protocol.internal.serialize([client_token, ID, partial])
        self.output_queue.send(payload)

        self._eof_seen.pop(client_token, None)
        self._by_client.pop(client_token, None)
        self._seen_data.pop(client_token, None)

    def process_messsage(self, message, ack, nack):
        try:
            logging.info("Process message")
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 4:
                self._process_data(fields[0], int(fields[1]), fields[2], int(fields[3]))
            elif len(fields) == 2:
                self._process_eof(fields[0], int(fields[1]))
            ack()
        except Exception:
            logging.exception("Aggregation failed processing message")
            nack()

    def _shutdown(self, signum, frame):
        logging.info("Aggregation received shutdown signal")
        try:
            self.input_exchange.stop_consuming()
        except Exception:
            logging.warning(
                "Aggregation: stop_consuming failed during shutdown", exc_info=True
            )

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
                logging.warning("Aggregation: failed to close input exchange", exc_info=True)
            try:
                self.output_queue.close()
            except Exception:
                logging.warning("Aggregation: failed to close output queue", exc_info=True)


def main():
    logging.basicConfig(level=logging.INFO)
    try:
        aggregation_filter = AggregationFilter()
        aggregation_filter.start()
    except Exception:
        logging.exception("Aggregation fatal error")
        return 1
    return 0


if __name__ == "__main__":
    main()
