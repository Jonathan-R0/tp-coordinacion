import logging
import os
import signal

from common import fruit_item, message_protocol, middleware

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])


def _merge_partials(partials):
    merged = {}
    for partial in partials:
        for fruit, amount in partial:
            cur = merged.get(fruit, fruit_item.FruitItem(fruit, 0))
            merged[fruit] = cur + fruit_item.FruitItem(fruit, int(amount))
    items = sorted(merged.values())
    items.reverse()
    top = items[:TOP_SIZE]
    return [[fi.fruit, fi.amount] for fi in top]


class JoinFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self._pending = {}

    def process_messsage(self, message, ack, nack):
        try:
            logging.info("Received partial top")
            client_token, aggregation_id, partial = message_protocol.internal.deserialize(message)
            bucket = self._pending.setdefault(client_token, {})
            bucket[int(aggregation_id)] = partial
            if len(bucket) < AGGREGATION_AMOUNT:
                ack()
                return

            merged = _merge_partials(bucket.values())
            out = message_protocol.internal.serialize([client_token, merged])
            self.output_queue.send(out)
            del self._pending[client_token]
            ack()
        except Exception:
            logging.exception("Join failed processing message")
            nack()

    def _shutdown(self, signum, frame):
        logging.info("Join received shutdown signal")
        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.warning("Join: stop_consuming failed during shutdown", exc_info=True)

    def start(self):
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            try:
                self.input_queue.close()
            except Exception:
                logging.warning("Join: failed to close input queue", exc_info=True)
            try:
                self.output_queue.close()
            except Exception:
                logging.warning("Join: failed to close output queue", exc_info=True)


def main():
    logging.basicConfig(level=logging.INFO)
    try:
        join_filter = JoinFilter()
        join_filter.start()
    except Exception:
        logging.exception("Join fatal error")
        return 1
    return 0


if __name__ == "__main__":
    main()
