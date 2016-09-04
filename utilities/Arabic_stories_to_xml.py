import pymongo
from pymongo import MongoClient
from lxml import etree

client = MongoClient()
db = client.lexisnexis
stories = db.gigaword
page = etree.Element('Sentences')
doc = etree.ElementTree(page)
doc_count=0;
for story in stories.find({"language" : "ARB"}).limit(1):
	sentenceElt=etree.SubElement(page, 'Sentence', date=story['publication_date'].strftime('%Y%m%d') , id=story['doc_id'], mongoId=str(story['_id']), source=story['news_source'], sentence="true")
	title = etree.SubElement(sentenceElt, 'Text')
	title.text = story['article_body']
	doc_count+=1
	print doc_count
	outFile = open('Arabic.xml', 'w')
doc.write(outFile)
