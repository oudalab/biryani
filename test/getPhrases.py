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

input_db='test'
output_db='test2'

f = open('/dev/null', 'w')
sys.stdout = f
#print sys.argv;
#input_db=sys.argv[1];
#output_db=sys.argv[2];
#exit(1);
config = petrarch2.utilities._get_data('data/config/', 'PETR_config.ini')
petrarch2.PETRreader.parse_Config(config)
petrarch2.read_dictionaries()
#Log
host='ip address'
port_number=5000
py_logger = logging.getLogger('python-logstash-logger')
py_logger.setLevel(logging.DEBUG)
py_logger.addHandler(logstash.TCPLogstashHandler(host, port_number, version=1))

#Mongo Db
client = MongoClient()
db=client.test_database
db_phrases = db.phrases

#Sqlite connections
conn = sqlite3.connect('test.db')
c = conn.cursor()
try:
    c.execute("SELECT id,date,output,mongo_id FROM json_test_table")
except:
    py_logger.error("Input database error");
    exit(1);

rows= c.fetchall()

for row in rows:
    phrases_output=[]
    date=date_formatter(row[1])
    doc_id=row[3]
    corenlpJsonData=json.loads(row[2])
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
            return_dict = petrarch2.do_coding(dict, None)
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
        print phrase_dict
        sen_phrases_dict={sentenceData: phrase_dict}
        phrases_output.append(sen_phrases_dict)
    db_phrases.insert({"doc_id":doc_id,"phrases":json.dumps(phrases_output)})
