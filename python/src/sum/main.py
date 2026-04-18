import logging
import os
import signal
import threading
import zlib

from common import fruit_item, message_protocol, middleware

SUM_EOF_FANOUT = "_eof_fanout"

class SumFilter:
    def __init__(self):
        self._id = int(os.environ["ID"])
        self._mom_host = os.environ["MOM_HOST"]
        self._input_queue_name = os.environ["INPUT_QUEUE"]
        self._sum_amount = int(os.environ["SUM_AMOUNT"])
        self._sum_prefix = os.environ["SUM_PREFIX"]
        self._aggregation_amount = int(os.environ["AGGREGATION_AMOUNT"])
        aggregation_prefix = os.environ["AGGREGATION_PREFIX"]

        self._state_lock = threading.Lock()
        self._amount_by_client_fruit = {}
        self._completed_flush = set()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            self._mom_host, self._input_queue_name
        )

        self.data_output_exchanges = []
        for i in range(self._aggregation_amount):
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                self._mom_host,
                aggregation_prefix,
                [f"{aggregation_prefix}_{i}"],
            )
            self.data_output_exchanges.append(exchange)

        self._fanout_pub = None
        self._fanout_sub = None
        self._coord_exchange_name = f"{self._sum_prefix}{SUM_EOF_FANOUT}"

        if self._sum_amount > 1:
            self._fanout_pub = middleware.MessageMiddlewareFanoutRabbitMQ(
                self._mom_host, self._coord_exchange_name
            )
            coord_queue = f"{self._sum_prefix}_coord_{self._id}"
            self._fanout_sub = middleware.MessageMiddlewareFanoutRabbitMQ(
                self._mom_host,
                self._coord_exchange_name,
                bind_queue_name=coord_queue,
            )

    def _aggregator_shard(self, fruit: str) -> int:
        return 0 if self._aggregation_amount <= 0 else zlib.crc32(fruit.encode("utf-8")) % self._aggregation_amount

    def _publish_coord_flush(self, client_token: str):
        self._fanout_pub.send(message_protocol.internal.serialize([client_token]))

    def _flush_client(self, client_token: str):
        with self._state_lock:
            if client_token in self._completed_flush:
                return
            fruits = self._amount_by_client_fruit.pop(client_token, {})

        for fruit, item in fruits.items():
            shard = self._aggregator_shard(fruit)
            payload = message_protocol.internal.serialize(
                [client_token, fruit, item.amount]
            )
            self.data_output_exchanges[shard].send(payload)

        eof_body = message_protocol.internal.serialize([client_token])
        for j in range(self._aggregation_amount):
            self.data_output_exchanges[j].send(eof_body)

        with self._state_lock:
            self._completed_flush.add(client_token)

    def _handle_gateway_eof(self, client_token: str):
        if self._sum_amount == 1:
            self._flush_client(client_token)
        else:
            self._publish_coord_flush(client_token)

    def _on_coord_flush(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_token = fields[0]
            self._flush_client(client_token)
            ack()
        except Exception:
            nack()

    def process_data_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                client_token, fruit, amount = fields
                with self._state_lock:
                    bucket = self._amount_by_client_fruit.setdefault(client_token, {})
                    current = bucket.get(
                        fruit, fruit_item.FruitItem(fruit, 0)
                    )
                    bucket[fruit] = current + fruit_item.FruitItem(
                        fruit, int(amount)
                    )
            elif len(fields) == 1:
                self._handle_gateway_eof(fields[0])
            ack()
        except Exception:
            nack()

    def _coord_loop(self):
        self._fanout_sub.start_consuming(self._on_coord_flush)

    def _shutdown(self, signum, frame):
        logging.info("Sum received shutdown signal")
        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass
        try:
            if self._fanout_sub:
                self._fanout_sub.stop_consuming()
        except Exception:
            pass

    def start(self):
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        if self._sum_amount > 1:
            threading.Thread(target=self._coord_loop, daemon=True).start()

        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            try:
                self.input_queue.close()
            except Exception:
                pass
            try:
                if self._fanout_pub:
                    self._fanout_pub.close()
            except Exception:
                pass
            try:
                if self._fanout_sub:
                    self._fanout_sub.close()
            except Exception:
                pass
            for exch in self.data_output_exchanges:
                try:
                    exch.close()
                except Exception:
                    pass


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
