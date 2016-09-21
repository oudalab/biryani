# -*- coding: utf-8 -*-
import pymongo
from pymongo import MongoClient
import xml.etree.ElementTree as etree

client = MongoClient()
db = client.lexisnexis
stories = db.gigaword
root = etree.Element('Articles')
doc_count=0;
for story in stories.find({"language" : "ARB"}).limit(500):
        sentenceElt=etree.SubElement(root, 'Article', date=story['publication_date'].strftime('%Y%m%d') , id=story['doc_id'], mongoId=str(story['_id']), source=story['news_source'], sentence="true")
        title = etree.SubElement(sentenceElt, 'Text')
        title.text = story['article_body']
        #print story['article_body']
        #exit(1)
        doc_count+=1
        print doc_count

tree = etree.ElementTree(root)
tree.write('sample2.xml','UTF-8')
