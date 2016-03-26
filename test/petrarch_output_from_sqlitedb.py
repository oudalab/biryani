import sqlite3
from bson import json_util
import json


conn = sqlite3.connect('test2.db')
c = conn.cursor()
c.execute("SELECT * FROM petrarch_table")
rows= c.fetchall()
for row in rows:
	doc_id= row[0].encode()
	data=row[1]
	mongo_id=row[2]
	num_sents=row[3]
	data= json.loads(data)
	data_array=json.loads(data['main_output'])
	#Code for events
	for element in data_array:
		element_json=json.loads(element)
		print element_json
		if 'sents' in element_json[mongo_id]:
			for i in range(1,num_sents+1):
				if element_json[mongo_id]['sents']!=None:
					if str(i) in element_json[mongo_id]['sents']:
						if 'events' in element_json[mongo_id]['sents'][str(i)]:
							print 'mongo_id: '+mongo_id
							print 'sen_id: '+str(i)
							print 'sentence:'+element_json[mongo_id]['sents'][str(i)]['content']
							print element_json[mongo_id]['sents'][str(i)]['events']
		#code for getting verbs	
		#if 'verbs' in element_json[mongo_id]['meta']:
		#	print element_json[mongo_id]['meta']['verbs']

		
