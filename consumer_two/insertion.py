#!/usr/bin/env python
import pika, sys, os, json
from pymongo import MongoClient

def main():

    client = MongoClient("mongodb://mongodb:27017")

    db = client.StudentManagement
    collection = db.students

    connection = pika.BlockingConnection(parameters= pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='insert_record', durable=True)

    def insert_record(ch, method, properties, body):
        data = json.loads(body)
        print("[x] Received %r" % data, flush=True)
        collection.insert_one(data)
        # time.sleep(body.count(b'.'))
        # print(" [x] Done")
        ch.basic_ack(delivery_tag = method.delivery_tag)

    channel.basic_consume(queue= "insert_record", on_message_callback=insert_record)

    print('[*]Insert Record: Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)



