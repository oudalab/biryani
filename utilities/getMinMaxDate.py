from pymongo import MongoClient
from bson.objectid import ObjectId
import re
def date_formatter(raw_date,doc_id):
	week_day=['SUNDAY','MONDAY','TUESDAY','WEDNESDAY','THURSDAY','FRIDAY','SATURDAY']
        try:
		date_split=raw_date.split(' ')
                date_split[0]=re.sub('[^A-Za-z0-9]+', '', date_split[0])
		print date_split[0]
		if (date_split[0].upper()) in week_day:
			date_split=date_split[1:]+date_split[0:1]
        	if bool(re.search(r'\d',date_split[0])):
                	day = date_split[0]
        	else:   
                	month = date_split[0]
        	
	        if bool(re.search(r'\d',date_split[1])) == False:
	                month = date_split[1]
	        else:   
	                day = date_split[1]
	        
	        month = re.sub('[^A-Za-z0-9]+', '', date_split[0])
	        month=month_to_num(month.upper())
	        
	        day= re.findall(r'\d+', day)
	        year = date_split[2]
	        year=re.findall(r'\d+', year)
	        #print day
	        if len(day[0]) <2:
	                day= '0'+day[0]
	        else:   
	                day = day[0]
	        
	        year = year[0]
		if int(year) > 2016:
			print str(doc_id)+"::"+raw_date
			return "-1"
	        return year+month+day	
	except:
		print str(doc_id)+"::"+raw_date
		return "-1"

	return "-1"
	
def month_to_num(month):
	return{
		'JANUARY':'01',
		'FEBRUARY':'02',
		'MARCH':'03',
		'APRIL':'04',
		'MAY':'05',
		'JUNE':'06',
		'JULY':'07',
		'AUGUST':'08',
		'SEPTEMBER':'09',
		'OCTOBER':'10',
		'NOVEMBER':'11',
		'DECEMBER':'12'

	}.get(month,'01')
	

client = MongoClient()
db= client['lexisnexis']
collection = db['disk_stories_full']

minDate = 99999999
maxDate =0
for doc in collection.find({"date_int":0}):
	doc_id = str(doc["_id"])
	publication_date_raw= doc['publication_date_raw']
	date=date_formatter(publication_date_raw,doc_id)
	if date != "-1":
		date= int(date)
		mongoUpdate = db.disk_stories_full.update_one({'_id' : ObjectId(doc_id)},{'$set':{'date_int':date}},upsert=False)
		if date > maxDate:
			maxDate = date
		if date < minDate:
			minDate = date
	else:
		mongoUpdate = db.disk_stories_full.update_one({'_id' : ObjectId(doc_id)},{'$set':{'date_int':0}},upsert=False)

print "min date::"+str(minDate)
print "max date::"+str(maxDate)
