#!/usr/bin/env python
import pika, sys, os, time

def main():
    connection = pika.BlockingConnection(parameters= pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='health_check', durable=True)

    def process_health_check(ch, method, properties, body):
        print("[x] Health checked: %r" % body.decode(), flush=True)
        # time.sleep(body.count(b'.'))
        # print(" [x] Health check")
        ch.basic_ack(delivery_tag = method.delivery_tag)

    channel.basic_consume(queue= "health_check", on_message_callback=process_health_check)

    print('[*] Health Check: Waiting for messages. To exit press CTRL+C')
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

