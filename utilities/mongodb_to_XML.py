import pymongo
from pymongo import MongoClient
from lxml import etree
import sys
import xml.etree.ElementTree as etree
client = MongoClient()
db = client.database #change database 
stories = db.collection #change collection
root = etree.Element('Articles')
 
doc_count=0
batch_count=0
total_doc_size = stories.find({"language" : "ARB"}).count()
batch_size= int(sys.argv[1])
 
for story in stories.find({"language" : "ARB"}):
        sentenceElt=etree.SubElement(root, 'Article', date=story['publication_date'].strftime('%Y%m%d') , id=story['doc_id'], mongoId=str(story['_id']), source=story['news_source'], sentence="true")
        title = etree.SubElement(sentenceElt, 'Text')
        title.text = story['article_body']
        doc_count+=1
        print doc_count
        if doc_count == batch_size:
                batch_count= batch_count+1;
                tree= etree.ElementTree(root)
                tree.write('gigaword_arabic_'+str(batch_count)+'.xml','UTF-8')
                doc_count=0
                root = etree.Element('Articles')
                total_doc_size = total_doc_size - batch_size
                if total_doc_size < batch_size:
                        batch_size = total_doc_size
