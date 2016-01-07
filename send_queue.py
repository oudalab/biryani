import pika
from bson import json_util
import json
from pymongo import MongoClient
# Code for Mongodb
client = MongoClient()
db = client.lexisnexis
stories=db.stories
#Code for RabbitMQ
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='main_queue', durable=True)

#Fetching data from MongoDB and sending to queue
for story in stories.find():
	
	message=json.dumps(story,default=json_util.default)
	channel.basic_publish(exchange='',
                      routing_key='main_queue',
                      body=message,
                      properties=pika.BasicProperties(
                         delivery_mode = 2,
          
            ))
	print message
connection.close()


