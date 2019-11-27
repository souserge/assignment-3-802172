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

    channel.basic_publish(
        exchange='', routing_key='hello', body='Hello World!')
    print(" [x] Sent 'Hello World!'")
    connection.close()


if __name__ == '__main__':
    print('Starting producer')
    main()
