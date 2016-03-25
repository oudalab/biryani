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
	data= json.loads(data)
	data_array=json.loads(data['main_output'])
	print 'Getting data of: '+doc_id	
	for element in data_array:
		element_json= json.loads(element)
		print element_json[mongo_id]['meta']
		
