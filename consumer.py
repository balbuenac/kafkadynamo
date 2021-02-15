from kafka import KafkaConsumer
from json import loads
from time import sleep
import boto3
import logging

def getItems():
    dynamodb = boto3.resource('dynamodb',aws_access_key_id='',
         aws_secret_access_key='', region_name='sa-east-1')
    table = dynamodb.Table('DummyResponses')
    response = table.scan()
    logging.info('Getting elemements %s', response)
    return response['Items']

if __name__ == '__main__':

    # Check if values is on redis cache. If not
    # get first values from dynamoDB and then put it in cache
    items = getItems()

    d = {}
    for item in items:
        d[item['reponse_id']] = item['Description']

    consumer = KafkaConsumer(
        'topic_test',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    while (True):
        for event in consumer:
            event_data = event.value
            data = event_data['expression']
            print(data)
            if data == '200':
                print(d[data])
            elif data == 'StringType':
                print(d[data])
            else:
                print('I dont know what is this')
            sleep(2)
