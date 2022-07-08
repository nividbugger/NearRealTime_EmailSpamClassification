from kafka import KafkaProducer
from json import dumps


def sendRecords(producer, topic, value):
    ''' Send records to Kafka topic '''
    try:
        producer.send(topic, value=value)
        producer.flush()
    except Exception as ex:
        print('Encountered error while publishing message')


def connectProducer():
    ''' Create Kafka Producer '''
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:6100'],
                                 value_serializer=lambda record: record.encode('utf-8'))
    except Exception as ex:
        print('Encountered error while publishing message')
    finally:
        return producer


def publishRecords(input, topic):
    ''' Iterate over files and send records '''
    if len(input) > 0:
        kafka_producer = connectProducer()
        for elem in input:
            sendRecords(kafka_producer, topic, elem.strip())
        if kafka_producer is not None:
            kafka_producer.close()
        