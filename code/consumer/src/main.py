# Builtin imports
import os

# 3rd party imports
import pika


RABBITMQ_HOST = os.environ['RABBITMQ_HOST']
RABBITMQ_USERNAME = os.environ['RABBITMQ_DEFAULT_USER']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_DEFAULT_PASS']


def main():
    credentials = pika.credentials.PlainCredentials(
        username=RABBITMQ_USERNAME, password=RABBITMQ_PASSWORD)

    connection_parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        connection_attempts=5,
        retry_delay=5)

    connection = pika.BlockingConnection(connection_parameters)

    channel = connection.channel()
    channel.queue_declare(queue='hello')

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)

    channel.basic_consume(
        queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    print('Starting consumer')
    main()
