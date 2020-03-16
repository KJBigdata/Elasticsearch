from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from selenium import webdriver
import time
import json
import glob
import re


class Els():

    # 객체 생성
    es = Elasticsearch(hosts="localhost", port=9200)
    # Tokenizer 생성
    indices_client = IndicesClient(es)
    # Download News crawling data into json file type : 각자에게 맞는 Path로 수정 요망
    path = "C:/Users/kjbig/.PyCharmCE2019.3/Elasticsearch/news"
    # Because of Crawling css selector for Comments #
    driver = webdriver.Chrome('C:/Program Files (x86)/Google/Chrome/chromedriver')
    base_url = "http://news.naver.com/#"


    @classmethod
    def srvHealthCheck(cls):
        # ===============
        # Check http://localhost:9200/_cat/health?v
        # ===============
        health = cls.es.cluster.health()
        print(health)

    @classmethod
    def allIndex(cls):
        # ===============
        # Elasticsearch에 있는 모든 Index 조회
        # ===============
        print(cls.es.cat.indices())

    @classmethod
    def createIndex(cls):
        # ===============
        # 인덱스 생성
        # 최초 1회만 사용할 것.
        # ===============
        cls.es.indices.create(
            index="searchnews",
            body={
                "settings": {
                    "number_of_shards": 2,
                    "index":{
                        "analysis":{
                            "analyzer":{
                                "nori_analyzer":{
                                "tokenizer":"nori_my_tokenizer",
                                "filter":["my_posfilter"]
                                }
                            },
                             "tokenizer":{
                                 "nori_my_tokenizer":{
                                     "type":"nori_tokenizer",
                                     "decompound_mode":"mixed"
                                 }
                             },
                            "filter":{
                                "my_posfilter":{
                                    "type":"nori_part_of_speech",
                                    "stoptags":["E", "IC","J","MAG", "MAJ",
                                                "MM", "SP", "SSC", "SSO", "SC",
                                                "SE", "XPN", "XSA", "XSN",
                                                "XSV", "UNA", "NA", "VSV"]
                                    }
                                }
                            }
                        }
                    },
                 "mappings": {
                         "properties": {
                             "category": {
                                 "type": "text",
                                 "fields": {
                                     "keyword": {
                                         "type": "keyword",
                                         "ignore_above": 256
                                     }
                                 }
                             },
                             "collect_time": {
                                 "type": "date"
                             },
                             "interest_cnt": {
                                 "type": "long"
                             },
                             "source": {
                                 "type": "text",
                                 "fields": {
                                     "keyword": {
                                         "type": "keyword",
                                         "ignore_above": 256
                                     }
                                 }
                             },
                             "title": {
                                 "type": "text",
                                 "analyzer":"nori_analyzer",
                                 "search_analyzer":"standard",
                                 "fields": {
                                     "keyword": {
                                         "type": "keyword",
                                         "ignore_above": 256
                                     }
                                 }
                             },
                             "title_tokens": {
                                 "type": "text",
                                 "analyzer": "nori_analyzer",
                                 "search_analyzer": "standard",
                                 "fields": {
                                     "keyword": {
                                         "type": "keyword",
                                         "ignore_above": 256
                                     }
                                 }
                             }
                       }
                 }
            }
        )

    @classmethod
    def dataInsert(cls):
        # ===============
        # Inserting json file(crawling data) documents into elasticsearch
        # ===============
        with open("{}/{}.json".format(cls.path,str(datetime.now())[:10]), "r", encoding="utf-8") as json_file:
            data = json.loads(json_file.read())
            for n, i in enumerate(data['crawling']):
                doc = {"source": i['source'],
                       "category": i["category"],
                       "title": i["title"],
                       "title_tokens": i["title_tokens"],
                       "interest_cnt": i["interest_cnt"],
                       "collect_time": i["collect_time"][:-7].replace(" ","T"),
                       #"article_body": i["article_body"]
                       }
                res = cls.es.index(index="searchnews", doc_type="_doc", body=doc)
                print(res)

    @classmethod
    def executeCheck(cls):
        # ===============
        # Checking creating index in kibana browser interface
        # ===============
        #cls.driver.get('localhost:9200')
        cls.driver.get('http://localhost:5601/app/kibana#/management/elasticsearch/index_management/indices?_g=()')
        time.sleep(15)

    @classmethod
    def searchAll(cls):
        # ===============
        # 데이터 조회 [전체]
        # ===============
        ix = cls.es.search(
            index = "searchnews", doc_type = "_doc",
            body = {
                "query":{"match_all":{}}
            }
        )
        print (json.dumps(ix, ensure_ascii=False, indent=4))
