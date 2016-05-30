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
channel.confirm_delivery()

#Fetching data from MongoDB and sending to queue
for story in stories.find({'queue_added':0}):

        doc_id=story['doc_id']
        message=json.dumps(story,default=json_util.default)
        if channel.basic_publish(exchange='',
                      routing_key='main_queue',
                      body=message,
                      properties=pika.BasicProperties(
                         delivery_mode = 2,

            )):
                print doc_id
                #code to make flag(queue_added=1) for the document
                #stories.update_one({"doc_id":doc_id},{"$set":{"queue_added":1}})
        else:
                print "failed to send: "+doc_id
connection.close()

