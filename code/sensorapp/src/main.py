# Builtin imports
import os
import csv

# 3rd party imports
import pika

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USERNAME = os.getenv('RABBITMQ_DEFAULT_USER', 'admin')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_DEFAULT_PASS', 'admin')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', 5672)
QUEUE_NAME = os.getenv('RABBITMQ_STREAMAPP_QUEUE_NAME', 'customerstreamapp')
DATA_PATH = os.getenv('DATA_PATH', '../../data/dataset/data.csv')
EXCHANGE_DEFAULT = ''  # must be blank


def main():
    def send_data(channel):
        channel.queue_declare(queue=QUEUE_NAME)

        def publish_to_rabbit(row):
            body = '\n'.join(row)

            channel.basic_publish(
                exchange=EXCHANGE_DEFAULT, routing_key=QUEUE_NAME, body=body)

        process_data_stream(publish_to_rabbit)

    use_rabbit_channel(send_data)


def use_rabbit_channel(callback):
    credentials = pika.credentials.PlainCredentials(
        username=RABBITMQ_USERNAME, password=RABBITMQ_PASSWORD)

    connection_parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        connection_attempts=5,
        retry_delay=5)

    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    callback(channel)

    connection.close()


def process_data_stream(processor):
    with open(DATA_PATH) as csvfile:
        data_stream = csv.reader(csvfile, strict=True)
        header = next(data_stream, None)
        for idx, row in enumerate(data_stream):
            processor(row)
            if idx > 10:
                break


if __name__ == '__main__':
    print('Starting sensor app')
    main()
