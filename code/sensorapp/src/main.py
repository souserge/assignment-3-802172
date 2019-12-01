# Builtin imports
import os
import csv

# 3rd party imports
import pika

RABBITMQ_HOST = os.environ['RABBITMQ_HOST']
RABBITMQ_USERNAME = os.environ['RABBITMQ_DEFAULT_USER']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_DEFAULT_PASS']

QUEUE_NAME = 'customerstreamapp'
DATA_PATH = '/app/data/data.csv'
EXCHANGE_DEFAULT = ''


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
        for row in data_stream:
            processor(row)


if __name__ == '__main__':
    print('Starting sensor app')
    main()
