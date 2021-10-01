import logging
import sys
import datetime
import json
import azure.functions as func
from confluent_kafka import Consumer,KafkaException,KafkaError
MIN_COMMIT_COUNT = 100



def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        running = True
        msg_count = 1
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                
                print(msg)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=False)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
    
def main(req: func.HttpRequest) -> func.HttpResponse:
    running = True
    
    topic =  ["bursatil_portafolio_historico".encode('utf-8')]
    conf = {'bootstrap.servers': 'pkc-4nym6.us-east-1.aws.confluent.cloud:9092',
            'group.id': "foo",
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}

    consumer = Consumer(conf)

    print(datetime.datetime.now())
        
    consume_loop(consumer,topic)

    return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
    )
