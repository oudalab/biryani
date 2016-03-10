import sqlite3
from bson import json_util
import json
from pymongo import MongoClient
# Code for Mongodb
client = MongoClient()
db = client.database
stories=db.collection
bulk = stories.initialize_unordered_bulk_op()
#Code for SQlite
conn = sqlite3.connect('database.db')
c = conn.cursor()
c.execute("query")
rows= c.fetchall()
for row in rows:
        doc_id= row[0]
        #data_json=json.loads(row[1])
        #for sentence in data_json['sentences']:
                #sen_dump=json.dumps(sentence)
                #sen_json=json.loads(sen_dump)
                #sen_id=sen_json['sen_id']
                #sen_data=sen_json['sentence']
                #print sen_id
                #print sen_data
        sentences=row[1]
        bulk.find({'doc_id': doc_id}).update({'$set': {'parsed_data': sentences}})
result= bulk.execute()

