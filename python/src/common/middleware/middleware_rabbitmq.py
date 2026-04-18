import pika
import logging
from .middleware import (
    MessageMiddleware,
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
)

logger = logging.getLogger(__name__)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host: str, queue_name: str):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

        self._connect()

    def _connect(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            logger.info(f"Connected to RabbitMQ. Queue declared: {self.queue_name}")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error connecting to RabbitMQ: {e}")
            raise MessageMiddlewareDisconnectedError("Could not connect to RabbitMQ")

    def send(self, message: str):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No active connection to RabbitMQ")
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=1),
            )
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise MessageMiddlewareMessageError(f"Error sending message: {e}")

    def start_consuming(self, on_message_callback):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No active connection to RabbitMQ")

            self.channel.basic_qos(prefetch_count=10)

            logger.info(f"Waiting messages on queue {self.queue_name}...")
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self._wrap_callback(on_message_callback),
                auto_ack=False,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Connection to RabbitMQ lost: {e}")
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost")
        except Exception as e:
            logger.error(f"Error during message consumption: {e}")
            raise MessageMiddlewareMessageError(f"Error during message consumption: {e}")

    def _wrap_callback(self, user_callback):
        def wrapper(ch, method, properties, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            try:
                user_callback(body, ack, nack)
            except Exception as e:
                logger.error(f"Error in callback: {e}")
                nack()

        return wrapper

    def stop_consuming(self):
        try:
            if self.channel:
                self.channel.stop_consuming()
                logger.info("Consumption stopped")
        except Exception as e:
            logger.error(f"Error stopping consumption: {e}")
            raise MessageMiddlewareMessageError(f"Error stopping consumption: {e}")

    def close(self):
        try:
            if self.connection:
                self.connection.close()
                logger.info("Connection to RabbitMQ closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
            raise MessageMiddlewareCloseError(f"Error closing connection: {e}")

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host: str, exchange_name: str, routing_keys: list):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys if isinstance(routing_keys, list) else [routing_keys]
        self.connection = None
        self.channel = None
        self.consumer_queue = None

        self._connect()

    def _connect(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)
            logger.info(f"Connected to RabbitMQ. Exchange declared: {self.exchange_name}")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error connecting to RabbitMQ: {e}")
            raise MessageMiddlewareDisconnectedError("Could not connect to RabbitMQ")

    def send(self, message: str, routing_key: str = None):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No active connection to RabbitMQ")

            key = routing_key if routing_key else self.routing_keys[0]

            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )
            logger.info(f"Message published to exchange: {self.exchange_name} with routing key {key}: {message}")
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise MessageMiddlewareMessageError(f"Error sending message: {e}")

    def start_consuming(self, on_message_callback):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No active connection to RabbitMQ")

            self.consumer_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue

            for route_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.consumer_queue,
                    routing_key=route_key,
                )

            self.channel.basic_qos(prefetch_count=1)

            logger.info(
                f"Waiting messages on exchange {self.exchange_name} with routing keys {self.routing_keys}..."
            )
            self.channel.basic_consume(
                queue=self.consumer_queue,
                on_message_callback=self._wrap_callback(on_message_callback),
                auto_ack=False,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Connection to RabbitMQ lost: {e}")
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost")
        except Exception as e:
            logger.error(f"Error during message consumption: {e}")
            raise MessageMiddlewareMessageError(f"Error during message consumption: {e}")

    def _wrap_callback(self, user_callback):
        def wrapper(ch, method, properties, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            try:
                user_callback(body, ack, nack)
            except Exception as e:
                logger.error(f"Error in callback: {e}")
                nack()

        return wrapper

    def stop_consuming(self):
        try:
            if self.channel:
                self.channel.stop_consuming()
                logger.info("Consumption stopped")
        except Exception as e:
            logger.error(f"Error stopping consumption: {e}")
            raise MessageMiddlewareMessageError(f"Error stopping consumption: {e}")

    def close(self):
        try:
            if self.connection:
                self.connection.close()
                logger.info("Connection to RabbitMQ closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
            raise MessageMiddlewareCloseError(f"Error closing connection: {e}")


class MessageMiddlewareFanoutRabbitMQ(MessageMiddleware):

    def __init__(self, host: str, exchange_name: str, bind_queue_name: str = None):
        self.host = host
        self.exchange_name = exchange_name
        self.bind_queue_name = bind_queue_name
        self.connection = None
        self.channel = None
        self._consumer_queue = None
        self._connect()

    def _connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host)
            )
            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=self.exchange_name, exchange_type="fanout", durable=True
            )
            logger.info(f"Connected to RabbitMQ fanout exchange: {self.exchange_name}")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error connecting to RabbitMQ: {e}")
            raise MessageMiddlewareDisconnectedError("Could not connect to RabbitMQ")

    def send(self, message):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No active connection to RabbitMQ")
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key="",
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except Exception as e:
            logger.error(f"Error publishing to fanout: {e}")
            raise MessageMiddlewareMessageError(f"Error publishing to fanout: {e}")

    def start_consuming(self, on_message_callback):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No active connection to RabbitMQ")
            result = self.channel.queue_declare(
                queue=self.bind_queue_name or "",
                exclusive=self.bind_queue_name is None,
            )
            self._consumer_queue = result.method.queue
            self.channel.queue_bind(
                exchange=self.exchange_name, queue=self._consumer_queue
            )
            self.channel.basic_qos(prefetch_count=1)
            logger.info(
                f"Waiting on fanout {self.exchange_name} queue {self._consumer_queue}..."
            )
            self.channel.basic_consume(
                queue=self._consumer_queue,
                on_message_callback=self._wrap_callback(on_message_callback),
                auto_ack=False,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Connection to RabbitMQ lost: {e}")
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost")
        except Exception as e:
            logger.error(f"Error during fanout consumption: {e}")
            raise MessageMiddlewareMessageError(f"Error during fanout consumption: {e}")

    def _wrap_callback(self, user_callback):
        def wrapper(ch, method, properties, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            try:
                user_callback(body, ack, nack)
            except Exception as e:
                logger.error(f"Error in fanout callback: {e}")
                nack()

        return wrapper

    def stop_consuming(self):
        try:
            if self.channel:
                self.channel.stop_consuming()
                logger.info("Fanout consumption stopped")
        except Exception as e:
            logger.error(f"Error stopping fanout consumption: {e}")
            raise MessageMiddlewareMessageError(f"Error stopping fanout consumption: {e}")

    def close(self):
        try:
            if self.connection:
                self.connection.close()
                logger.info("Fanout connection closed")
        except Exception as e:
            logger.error(f"Error closing fanout connection: {e}")
            raise MessageMiddlewareCloseError(f"Error closing fanout connection: {e}")
