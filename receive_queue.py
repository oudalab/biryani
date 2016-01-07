import pika
from bson import json_util
import json

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue='main_queue', durable=True)
print('Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
	data_json=json.loads(body)
	doc_id=data_json['doc_id']
	article_body= data_json['article_body']
	print doc_id
	print article_body
	ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='main_queue')

channel.start_consuming()
