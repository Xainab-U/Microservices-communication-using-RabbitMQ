#!/usr/bin/env python
import pika, sys, os, json
from pymongo import MongoClient

def main():

    client = MongoClient("mongodb://mongodb:27017")

    db = client.StudentManagement
    collection = db.students

    connection = pika.BlockingConnection(parameters= pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='delete_record', durable=True)

    def delete_record(ch, method, properties, body):
        data = json.loads(body)
        print("[x] Received %r" % data, flush=True)
        print("Deleted Details: ", (collection.delete_one(data)), flush=True)
        # print("Found Details: ", list(collection.find({})))
        # time.sleep(body.count(b'.'))
        # print(" [x] Done")
        ch.basic_ack(delivery_tag = method.delivery_tag)

    channel.basic_consume(queue= "delete_record", on_message_callback=delete_record,auto_ack=True)

    print('[*]Delete Record: Waiting for messages. To exit press CTRL+C')
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



