from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys


try:
    topic_in  = sys.argv[1]   # tópico de entrada (topic1)
    topic_out = sys.argv[2]   # tópico de saída  (topic2)
except:
    print('Usage: python3 consumer_producer.py <topic_in> <topic_out>')
    exit(1)

consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest')
consumer.subscribe([topic_in])


producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

print(f'Forwarding messages from [{topic_in}] to [{topic_out}]...')

for msg in consumer:
    original = msg.value.decode()
    print(f'Received from {topic_in}: {original}')

    processed = f'[PROCESSED] {original}'

    producer.send(topic_out, value=processed.encode())
    producer.flush()
    print(f'Forwarded to {topic_out}: {processed}')