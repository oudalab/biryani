from multiprocessing import Pool
import contextlib
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
import pymongo
from pymongo import MongoClient
from date_formatter import date_formatter

def get_phrases(doc):
    phrases_output=[]
    nouns=[]
    noun_coding=[]
    verbs=[]
    verb_coding=[]
    return_dict={}
    article_id=doc[0]
    date=date_formatter(doc[1])
    #logger.info('Date: '+date)
    doc_id=doc[3]
    corenlpJsonData=json.loads(doc[2])
    sentences=corenlpJsonData['sentences']
    for sentence in sentences:
        sen_phrases_dict = {}
        sentenceJson= json.loads(json.dumps(sentence))
        sentenceId=sentenceJson['sen_id']
        sentenceTree=sentenceJson['tree']
        sentenceData=sentenceJson['sentence']
        parsed=utilities._format_parsed_str(sentenceTree)
        dict = {doc_id: {u'sents': {sentenceId: {u'content': sentenceData, u'parsed': parsed}}, u'meta': {u'date': date.encode()}}}
        try:
            return_dict = petrarch2.do_coding(dict)
            n = return_dict[doc_id]['meta']['verbs']['nouns']
            nouns = [i[0] for i in n]
            noun_coding = [i[1] for i in n]
            verbs = return_dict[doc_id]['meta']['verbs']['eventtext'].values()[0]
        except:
            print "No eventtext"
            verbs = ""
        try:
            verb_coding = return_dict[doc_id]['meta']['verbs']['eventtext'].keys()[0][2]
        except KeyError as e:
            print e
            verb_coding = ""
        phrase_dict = {"nouns": nouns,
                       "noun_coding": noun_coding,
                       "verbs": verbs,
                       "verb_coding": verb_coding}
	sen_phrases_dict={sentenceData: phrase_dict}
        phrases_output.append(sen_phrases_dict)
        #print phrases_output
    return (article_id, json.dumps(phrases_output),doc_id)

if __name__ == '__main__':
	input_db=sys.argv[1]
	batch_size = int(sys.argv[2])
        threads = int(sys.argv[3])
	#Timing details
	total_time_start=timeit.default_timer()
	batch_time_start=timeit.default_timer()
		
	host = 'host ip'
	logger = logging.getLogger('python-logstash-logger')
	logger.setLevel(logging.INFO)
	logger.addHandler(logstash.TCPLogstashHandler(host,5000, version=1))
	
	logger.info('Petrarch started.............')
	f = open('/dev/null', 'w')
	sys.stdout = f
	config = petrarch2.utilities._get_data('data/config/', 'PETR_config.ini')
	petrarch2.PETRreader.parse_Config(config)
	petrarch2.read_dictionaries()
	
	#Mongo Db
	client = MongoClient()
	db=client.test_database
	db_phrases = db.phrases
	
	#sqlite
	conn2 = sqlite3.connect(input_db+'_petrarch.db')
	c2=conn2.cursor()
	c2.execute('''CREATE TABLE IF NOT EXISTS petrarch_table (doc_id varchar,phrases varchar,mongo_id varchar)''')
	total_corenlp_rows=0
	#Sqlite connections
	conn = sqlite3.connect(input_db)
	c = conn.cursor()
	c3 = conn.cursor()
	try:
	    rows = c.execute("SELECT id,date,output,mongo_id FROM json_test_table order by rowid desc")
	    total_rows = c3.execute("SELECT max(rowid) from json_test_table")
	except:
	    logger.error("Input database error");
	    exit(1);
	
	#rows= c.fetchall()
	for data in total_rows:
		total_corenlp_rows= data[0]
	logger.info('Total Rows to be processed: '+str(total_corenlp_rows))
	
	inserted_rows=0
	output=[]
	if(total_corenlp_rows< batch_size):
	        batch_size = total_corenlp_rows
	
	input_data=[]
	for row in rows:
		#print 'Got data'
		input_data.append(row)
		if(len(input_data)==batch_size):
			with contextlib.closing(Pool(processes=threads)) as p:
				output=p.map(get_phrases,input_data)
			input_data[:]=[]
		    	c2.executemany("""INSERT INTO petrarch_table(doc_id, phrases, mongo_id) VALUES (?,?,?)""",output)
		    	conn2.commit()
                        #exit(1)
		        batch_time_end=timeit.default_timer()
		        batch_time_taken=batch_time_end-batch_time_start
		    	inserted_rows+= batch_size
			output=[]
		        remaining_rows = total_corenlp_rows - inserted_rows
		    	logger.info('Total rows: '+str(total_corenlp_rows)+' Inserted Rows '+str(inserted_rows)+' Remaining Rows '+str(remaining_rows)+' Time Taken:'+str(batch_time_taken))
	    		if(remaining_rows<= batch_size):
		     		batch_size = remaining_rows
		#batch_time_start=timeit.default_timer()
	total_time_end=timeit.default_timer()
	total_time_taken=total_time_end - total_time_start
	logger.info('Total rows: '+str(total_corenlp_rows)+' Inserted Rows '+str(inserted_rows)+' Remaining Rows '+str(remaining_rows)+' Time Taken:'+str(total_time_taken))
