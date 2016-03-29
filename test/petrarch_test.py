from petrarch2 import petrarch2, PETRglobals, PETRreader, utilities
from petrarch2 import PETRtree as ptree
import sqlite3
import logging
import logstash
import sys
from bson import json_util
import json
from pymongo import MongoClient
from date_formatter import date_formatter

config = petrarch2.utilities._get_data('data/config/', 'PETR_config.ini')
petrarch2.PETRreader.parse_Config(config)
petrarch2.read_dictionaries()
#Log
host=''
port_number=0
py_logger = logging.getLogger('python-logstash-logger')                        
py_logger.setLevel(logging.DEBUG)
py_logger.addHandler(logstash.TCPLogstashHandler(host, port_number, version=1))

#Sqlite connections
conn = sqlite3.connect('test.db')
conn2 = sqlite3.connect('test2.db')
c = conn.cursor()
c2=conn2.cursor()
c.execute("SELECT * FROM json_test_table")
c2.execute('''CREATE TABLE IF NOT EXISTS petrarch_table
             (doc_id varchar,output varchar,mongo_id varchar,sents_count integer )''')
#count of rows inserted 
docs_inserted=0;

rows= c.fetchall()
docs_present= len(rows)
for row in rows:
	doc_id= row[0].encode()
	date=date_formatter(row[1])
        data_json=json.loads(row[2])
	mongo_id= row[3].encode()
 	sen_out_dict= {}
	sen_out_records= []
	sentences=data_json['sentences']
	sen_failed=0;
	sen_parsed=0;
	for sentence in sentences:
		sen_dump=json.dumps(sentence)
		sen_json=json.loads(sen_dump)
		sen_id=sen_json['sen_id'].encode()
		sen_data=sen_json['sentence']
		sen_parse=sen_json['tree']
		text=sen_data
		parse=sen_parse
		parsed = utilities._format_parsed_str(parse)
                try:
			py_logger.debug('parsing : '+doc_id)			
			dict = {mongo_id: {u'sents': {sen_id: {u'content': text, u'parsed': parsed}},
                		u'meta': {u'date': date.encode()}}}
			return_dict = petrarch2.do_coding(dict,None)
			return_dict= json.dumps(return_dict)
			#print return_dict
			output=return_dict
			sen_out_records.append(output)
			sen_parsed= sen_parsed+1;
                except:
			sen_failed=sen_failed+1;
			py_logger.error('Parsing failed: '+doc_id+' sen_id: '+sen_id)
	#code for inserting to Sqlite db
	sen_out_dict['main_output']=json.dumps(sen_out_records)
        c2.execute("""INSERT INTO petrarch_table(doc_id, output, mongo_id,sents_count) 
               VALUES (?,?,?,?)""", (doc_id, json.dumps(sen_out_dict), mongo_id,len(sentences)))
	conn2.commit()
	docs_inserted=docs_inserted+1;
	py_logger.debug('doc_id: '+doc_id+' Total sentences: '+ str(len(sentences)) +' #Parsed: '+str(sen_parsed)+' #Failed: '+str(sen_failed)+' #Doc: '+str(docs_inserted))
py_logger.debug('#Docs present '+str(docs_present)+' #Docs inserted '+str(docs_inserted))	
c2.close()
