import datetime
import sqlite3
from bson import json_util
from bson.objectid import ObjectId
import json
from pymongo import MongoClient
#code for timing
start_time=datetime.datetime.now()
# Code for Mongodb
client = MongoClient()
db = client.database
stories=db.collection
bulk = stories.initialize_unordered_bulk_op()
#Code for SQlite
conn = sqlite3.connect('test.db')
c = conn.cursor()
c.execute("select * from json_test_table")
rows= c.fetchall()
for row in rows:
        mongo_id= row[3]
        #data_json=json.loads(row[1])
        #for sentence in data_json['sentences']:
                #sen_dump=json.dumps(sentence)
                #sen_json=json.loads(sen_dump)
                #sen_id=sen_json['sen_id']
                #sen_data=sen_json['sentence']
                #print sen_id
                #print sen_data
        sentences=row[2]
        bulk.find({'_id': ObjectId(mongo_id)}).update({'$set': {'corenlp_output': sentences}})
result= bulk.execute()
end_time=datetime.datetime.now()
time_taken=start_time-end_time
print time_taken.total_seconds()
