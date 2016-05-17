import pika
from bson import json_util
import json
import sqlite3

# Code for Sqlite
conn = sqlite3.connect('test.db')
c = conn.cursor()
c.execute("SELECT * FROM json_test_table")
rows= c.fetchall()


#Code for RabbitMQ
with open('Sqlite_Rabbitmq.txt') as data_file:    
    data = json.load(data_file)
rabbitmq_user=data['rabbitmq_user']
rabbitmq_pass=data['rabbitmq_pass']
queue_name=data['queue_name']
credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=queue_name, durable=True)
channel.confirm_delivery()

#Fetching data from MongoDB and sending to queue
for row in rows:

        doc_id= row[0].encode()
        message=json.dumps(row,default=json_util.default)
        if channel.basic_publish(exchange='',
                      routing_key=queue_name,
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
