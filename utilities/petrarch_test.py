from petrarch2 import petrarch2, PETRglobals, PETRreader, utilities
from petrarch2 import PETRtree as ptree
import sqlite3
import logging
import logstash
import timeit
import os
import sys
from bson import json_util
import json
from date_formatter import date_formatter

f = open('/dev/null', 'w')
sys.stdout = f

config = petrarch2.utilities._get_data('data/config/', 'PETR_config.ini')
petrarch2.PETRreader.parse_Config(config)
petrarch2.read_dictionaries()
#Log
host='129.15.78.182'
port_number=5000
py_logger = logging.getLogger('python-logstash-logger')
py_logger.setLevel(logging.DEBUG)
py_logger.addHandler(logstash.TCPLogstashHandler(host, port_number, version=1))

#Timing details
total_time_start=timeit.default_timer()
batch_time_start=timeit.default_timer()


#Sqlite connections
conn = sqlite3.connect('test.db')
c = conn.cursor()
c.execute("SELECT * FROM json_test_table")

conn2 = sqlite3.connect('test2.db')
c2=conn2.cursor()
c2.execute('''CREATE TABLE IF NOT EXISTS petrarch_table (doc_id varchar,output varchar,mongo_id varchar,sents_count integer )''')
rows= c.fetchall()

#count of rows inserted
docs_inserted=0;
docs_present= len(rows)
remaining_docs=docs_present
batch_size=5000
batch_loop=docs_present/batch_size;
batch_insert_count=0;
output_tuple=()
output_records=[]
if batch_size>docs_present:
    batch_size=docs_present

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
			        dict = {mongo_id: {u'sents': {sen_id: {u'content': text, u'parsed': parsed}},u'meta': {u'date': date.encode()}}}
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
    py_logger.debug('doc_id: ' + doc_id + ' Total sentences: ' + str(len(sentences)) + ' #Parsed: ' +
                    str(sen_parsed) + ' #Failed: ' + str(sen_failed))
    sen_out_dict['main_output']=json.dumps(sen_out_records)
    output_tuple=(doc_id,json.dumps(sen_out_dict), mongo_id,len(sentences))
    output_records.append(output_tuple)
    output_records_length=len(output_records)
    if(output_records_length==batch_size):
        c2.executemany("""INSERT INTO petrarch_table(doc_id, output, mongo_id,sents_count) VALUES (?,?,?,?)""",
                        output_records)
        conn2.commit()
        batch_time_end=timeit.default_timer()
        batch_time_taken=batch_time_end-batch_time_start
        docs_inserted=docs_inserted+output_records_length
        batch_docs_inserted = output_records_length
        remaining_docs=remaining_docs-batch_docs_inserted
        batch_insert_count=batch_insert_count+1
        if(batch_insert_count==batch_loop):
            batch_size=remaining_docs
        py_logger.debug('#Batch_docs '+str(len(output_records))+' Time: '+str(batch_time_taken)+ ' secs'+
                        ' #Docs present ' + str(docs_present) + ' #Docs inserted ' + str(docs_inserted)) 
        batch_time_start=timeit.default_timer()
        output_records = []
        output_tuple = ()
c2.close()
total_time_end=timeit.default_timer()
total_time_taken=total_time_end - total_time_start
py_logger.debug("Total time: "+str(total_time_taken)+' secs'+'Total docs: '+str(docs_present)+' Total inserted: '+str(docs_inserted))
