from __future__ import unicode_literals
from __future__ import print_function
from bs4 import BeautifulSoup
import re
import dateutil.parser
import os
from bson import json_util
from pymongo import MongoClient
import datetime
import logging
import sys
import pika
import json
import random

queueName = "gigaword_VM"

credentials = pika.PlainCredentials('boomer', 'burritos_for_breakfast')
parameters = pika.ConnectionParameters('129.15.78.186', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel_rabbit = connection.channel()
channel_rabbit.queue_declare(queue=queueName, durable=True)
channel_rabbit.confirm_delivery()

queue_count = 0
batchSize = 12484

#logger = logging.getLogger()
#logger.setLevel(logging.DEBUG)

import argparse

parser = argparse.ArgumentParser("Traverse directories finding Gigaword files. Load into MongoDB")
parser.add_argument("--dir", help="path to Gigaword files for import", required=True)
args = parser.parse_args()
DIRECTORY = args.dir
print(DIRECTORY)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s -  %(message)s')
ch.setFormatter(formatter)
#logger.addHandler(ch)

connection = MongoClient()
db = connection.lexisnexis
collection = db['gigaword_doctype']


def extract_xml(doc):
    doc_id = doc['id']
    try:
        article_body = doc.find("text").text.strip()
    except AttributeError as e:
        #logger.warning("No article body for {}".format(doc_id))
        # print("No article body for {}".format(doc_id))
        article_body = ""
    word_count = len(article_body.split())
    try:
        article_title = doc.find("headline").text.strip()
    except AttributeError:
        #logger.warning("No headline for {}".format(doc_id))
        article_title = ""
    language = re.findall("_([A-Z]{3})_", doc_id)[0]
    news_source = re.findall("^([A-Z]{3})_", doc_id)[0]
    date_string = re.findall("_(\d{8})", doc_id)[0]
    parsed_date = dateutil.parser.parse(date_string)
    try:
        doc_type = doc['type']
    except Exception as e:
        #logger.info("Error getting story_type: {}".format(e))
        # print("Error getting story_type: {}".format(e))
        doc_type = ""
    try:
        dateline = doc.find("dateline").text.strip()
    except AttributeError:
        dateline = ""
        # logger.info("No dateline for doc {}".format(doc_id))
        # print("No dateline for doc {}".format(doc_id))
    doc_dict = {
        'article_title': article_title,
        'dateline': dateline,
        'article_body': article_body,
        'doc_id': doc_id,
        'publication_date': parsed_date,
        'news_source': news_source,
        'language': language,
        'doc_type': doc_type,
        'word_count': word_count,
        'type': doc_type
    }
    return doc_dict


def get_file_list(root_dir):
    files = []  # Will have list of all the files parsed through
    for dname, subdirlist, flist in os.walk(root_dir):
        for fname in flist:
            files.append(os.path.join(dname, fname))
    return files


def write_to_mongo(collection, doc):
    toInsert = {"news_source": doc['news_source'],
                "article_title": doc['article_title'],
                "publication_date": doc['publication_date'],
                "date_added": datetime.datetime.utcnow(),
                "article_body": doc['article_body'],
                "stanford": 0,
                "language": doc['language'],
                "doc_id": doc['doc_id'],
                'word_count': doc['word_count'],
                'dateline': doc['dateline'],
                'type': doc['type']
                }
    object_id = collection.insert(toInsert)
    return object_id


def write_to_rabbitmq(channel, doc):
    global queue_count
    story = {"news_source": doc['news_source'],
             "article_title": doc['article_title'],
             "publication_date": doc['publication_date'],
             "date_added": datetime.datetime.utcnow(),
             "article_body": doc['article_body'],
             "stanford": 0,
             "language": doc['language'],
             "doc_id": doc['doc_id'],
             'word_count': doc['word_count'],
             'dateline': doc['dateline'],
             'type': doc['type']
             }

    message = json.dumps(story, default=json_util.default)
    if channel.basic_publish(exchange='',routing_key=queueName,body=message,properties=pika.BasicProperties(delivery_mode=2,)):
        queue_count+=1
        print (queue_count)
        print ("Success $$$$")

        if queue_count == batchSize:
            exit(1)
    else:
        print ("failed to send")


def process_file(fi):
    # print("Processing file {}".format(fi))
    #logger.info("Processing file {}".format(fi))
    with open(fi, 'r') as f:
        docs = f.read()
    soup = BeautifulSoup(docs, "html.parser")
    # print(soup)
    for dd in soup.find_all("doc"):
        # logger.debug("found doc")
        try:
            ex = extract_xml(dd)
            if ex:
                try:
                    # logger.debug(ex['article_title'])
                    # write_to_mongo(collection, ex)
                    write_to_rabbitmq(channel_rabbit, ex)
                except Exception as e:
                    #logger.error("Mongo error: {0}".format(e), exc_info=True)
                    print("Mongo error: {0}".format(e))
            else:
                #logger.error("No extracted xml")
                print("No extracted xml")
        except IndexError as e:
            #logger.error("Problem (Index) extracting XML: {}".format(e), exc_info=True)
            print(e, exc_info=True)
            # print dd['filename']
            # print dd['xml']
        except AttributeError as e:
            #logger.error("Problem (Attribute) extracting XML: {}".format(e), exc_info=True)
            print(e, exc_info=True)
        except Exception as e:
            #logger.error("Some other error in extracting XML: ".format(e), exc_info=True)
            print("Some other error in extracting XML: ".format(e), exc_info=True)
    sys.stdout.flush()


if __name__ == "__main__":
    print("Traversing directory to get files...")
    #    DIRECTORY = parser.parse_args()
    ln_files = get_file_list(DIRECTORY)
    print("Found {0} documents".format(len(ln_files)))
    short_files = random.sample(ln_files,len(ln_files)) # [0:20] #[0:100]
    print("Extracting documents from {0} documents".format(len(short_files)))
    # pool_size = 4
    # print("Using {} workers".format(pool_size))
    # pool = multiprocessing.Pool(pool_size)
    print("ETLing...")
    # processed = [process_file(f) for f in short_files]
    # multiprocessing.log_to_stderr()
    # logger = multiprocessing.get_logger()
    # logger.setLevel(logging.INFO)
    # processed = [pool.map(process_file, short_files)]
    processed = [process_file(f) for f in short_files]
    print("Complete")



