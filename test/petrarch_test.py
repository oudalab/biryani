from petrarch2 import petrarch2, PETRglobals, PETRreader, utilities
from petrarch2 import PETRtree as ptree
import sqlite3
from bson import json_util
import json
from pymongo import MongoClient
from date_formatter import date_formatter
config = petrarch2.utilities._get_data('data/config/', 'PETR_config.ini')
#print("reading config")
petrarch2.PETRreader.parse_Config(config)
#print("reading dicts")
petrarch2.read_dictionaries()


# Code for Mongodb
client = MongoClient()
db = client.lexisnexis
stories=db.stories
bulk = stories.initialize_unordered_bulk_op()
#Code for SQlite
conn = sqlite3.connect('test.db')
conn2 = sqlite3.connect('test2.db')
c = conn.cursor()
c2=conn2.cursor()
c.execute("SELECT * FROM json_test_table")
c2.execute('''CREATE TABLE IF NOT EXISTS petrarch_table
             (doc_id varchar,output varchar,mongo_id varchar )''')
rows= c.fetchall()
for row in rows:
	print 'Document parsing'
	doc_id= row[0].encode()
	date=date_formatter(row[1])
        data_json=json.loads(row[2])
	mongo_id= row[3].encode()
 	sen_out_dict= {}
	sen_out_records= []
	for sentence in data_json['sentences']:
		sen_dump=json.dumps(sentence)
		sen_json=json.loads(sen_dump)
		sen_id=sen_json['sen_id'].encode()
		sen_data=sen_json['sentence']
		sen_parse=sen_json['tree']
		text=sen_data
		parse=sen_parse
		parsed = utilities._format_parsed_str(parse)
                try:

			
			dict = {mongo_id: {u'sents': {sen_id: {u'content': text, u'parsed': parsed}},
                		u'meta': {u'date': date.encode()}}}
			return_dict = petrarch2.do_coding(dict,None)
			return_dict= json.dumps(return_dict)
			#print return_dict
			output=return_dict
			sen_out_records.append(output)
                except:
			print "****************************************************Unexpected error****************************************"

	#code for inserting to Sqlite db
	#print len(sen_out_records)
	sen_out_dict['main_output']=json.dumps(sen_out_records)
        c2.execute("""INSERT INTO petrarch_table(doc_id, output, mongo_id) 
               VALUES (?,?,?)""", (doc_id, json.dumps(sen_out_dict), mongo_id))
	conn2.commit()
c2.close()
