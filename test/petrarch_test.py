
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
c = conn.cursor()
c.execute("SELECT * FROM json_test_table")
rows= c.fetchall()
for row in rows:
	doc_id= row[0].encode()
	#print doc_id
	date=date_formatter(row[1])
        #print row[2]
	data_json=json.loads(row[2])
	#print doc_id
	#print row[1]
	#print date
	#print data_json
	for sentence in data_json['sentences']:
		sen_dump=json.dumps(sentence)
		sen_json=json.loads(sen_dump)
		sen_id=sen_json['sen_id']
		sen_data=sen_json['sentence']
		sen_parse=sen_json['tree']
		text=sen_data
		parse=sen_parse
		parsed = utilities._format_parsed_str(parse)
                try:

			
			dict = {doc_id: {u'sents': {u'0': {u'content': text, u'parsed': parsed}},
                		u'meta': {u'date': date.encode()}}}
			return_dict = petrarch2.do_coding(dict,None)
			print(return_dict)
	
                except:
			print "Unexpected error"
                #print len(return_dict)
	
