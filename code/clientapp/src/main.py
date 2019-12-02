# Builtin imports
import os
from datetime import date

# 3rd party imports
import pika


RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USERNAME = os.getenv('RABBITMQ_DEFAULT_USER', 'admin')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_DEFAULT_PASS', 'admin')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', 5672)
EXCHANGE_TYPE_TOPIC = 'topic'  # must be blank
EXCHANGE_NAME = 'movement_notifications'


def notify(channel, binding_key, callback):
    channel.exchange_declare(
        exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE_TOPIC)

    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(
        exchange=EXCHANGE_NAME, queue=queue_name, routing_key=binding_key)

    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    print('[*] {}: Waiting for notifications. To exit press CTRL+C'.format(binding_key))
    channel.start_consuming()


def main():
    def consume_notifications(connection):
        def callback(ch, method, properties, body):
            [ts, moves] = body.split(',')
            da = date.fromisoformat(ts)
            print('Person {} moved {} times bewteen the rooms on the day of {}'.format(
                method.routing_key, moves, da.isoformat()))

        notify(connection.channel(), '#', callback)

    connect_to_rabbit(consume_notifications)


def connect_to_rabbit(callback):
    credentials = pika.credentials.PlainCredentials(
        username=RABBITMQ_USERNAME, password=RABBITMQ_PASSWORD)

    connection_parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        connection_attempts=5,
        retry_delay=5)

    connection = pika.BlockingConnection(connection_parameters)
    callback(connection)
    connection.close()


if __name__ == '__main__':
    print('Starting consumer')
    main()
