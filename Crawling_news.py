from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from selenium import webdriver
import time
import json
import glob
import re


class Ela():

    # 객체 생성
    es = Elasticsearch(hosts="localhost", port=9200)
    # Download News crawling data into json file type : 각자에게 맞는 Path로 수정 요망
    path = "C:/Users/kjbig/.PyCharmCE2019.3/Crawling"
    # Because of Crawling css selector for Comments #
    driver = webdriver.Chrome('C:/Program Files (x86)/Google/Chrome/chromedriver')
    base_url = "http://news.naver.com/#"

    @classmethod
    def srvHealthCheck(self):
        health = self.es.cluster.health()
        print(health)

    @classmethod
    def allIndex(self):
        # ===============
        # Elasticsearch에 있는 모든 Index 조회
        # ===============
        print(self.es.cat.indices())

    @classmethod
    def collecting(self):
        # ===============
        # 데이터 크롤링 [네이버 뉴스]
        # ===============
        data = urlopen(self.base_url).read()
        soup = BeautifulSoup(data, "html.parser")
        total_data = soup.find_all(attrs={'class': 'main_component droppable'})

        category = ""

        multiple_dic = {}
        multiple_dic['crawling'] = []

        for each_category in total_data:
            try:
                category = str(each_category.find_all(attrs={"class": "tit_sec"})).split(">")[2][:-3]
            except:
                pass

            data = str(each_category.find_all(attrs={"class": "mlist2 no_bg"})).split("<li>")

            for i,each_news in enumerate(data[1:]):
                print('-----------{}번째 {} Crawling-----------'.format(i + 1,category))
                new_block = each_news.split('href="')[1]
                title = new_block.split("<strong>")[1].split("</strong>")[0]
                collect_time = str(datetime.utcnow())
                link = new_block.split('"')[0].replace("amp;", "")
                soup2 = BeautifulSoup(urlopen(link).read(), "html.parser")
                self.driver.get(link)
                self.driver.implicitly_wait(30)
                print(link)

                # 해당 기사에 대한 총 댓글 수
                try:
                    interest_cnt = int(self.driver.find_element_by_css_selector('span.u_cbox_count').text.replace(',',''))
                except:
                    interest_cnt = int(self.driver.find_element_by_css_selector('em.simplecmt_num').text.replace(',',''))
                #article_body = str(soup2.find_all(attrs={"id": "articleBodyContents"}))
                print(interest_cnt)

                insert_data = {"source": "naver_news",
                               "category": category,
                               "title": title,
                               #"article_body": article_body,
                               "interest_cnt":interest_cnt,
                               "collect_time": collect_time}
                multiple_dic['crawling'].append(insert_data)

        if len(glob.glob('{}/{}.json'.format(self.path,str(datetime.now())[:10]))) == 1:
            with open("{}/{}.json".format(self.path,str(datetime.now())[:10]), "w", encoding="utf-8") as output_file:
                json.dump(multiple_dic,output_file,indent=4)
        else:
            with open("{}/{}.json".format(self.path,str(datetime.now())[:10]), "a", encoding="utf-8") as output_file:
                json.dump(multiple_dic,output_file,indent=4)
        return multiple_dic

    @classmethod
    def createIndex(self):
        # ===============
        # 인덱스 생성
        # ===============
        self.es.indices.create(
            index="searchnews",
            body={
                "settings": {
                    "number_of_shards": 2,
                    "index":{
                        "analysis":{
                            "analyzer":{
                                "nori_analyzer":{
                                "type":"custom",
                                "tokenizer":"nori_user_dict",
                                "filter":["my_posfilter"]
                                }
                            },
                            "tokenizer":{
                                "nori_user_dict":{
                                    "type":"nori_tokenizer",
                                    "decompound_mode":"mixed",
                                    "user_dictionary":"userdict_ko.txt"
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
    def dataInsert(self):
        # ===============
        # 데이터 삽입
        # ===============
        with open("{}/{}.json".format(self.path,str(datetime.now())[:10]), "r", encoding="utf-8") as json_file:
            data = json.loads(json_file.read())
            for n, i in enumerate(data['crawling']):
                doc = {"source": i['source'],
                       "category": i["category"],
                       "title": i["title"],
                       "interest_cnt": i["interest_cnt"],
                       "collect_time": i["collect_time"][:-7].replace(" ","T")}
                res = self.es.index(index="searchnews", doc_type="_doc", body=doc)
                print(res)

    @classmethod
    def ExecuteCheck(self):
        #self.driver.get('localhost:9200')
        self.driver.get('http://localhost:5601/app/kibana#/management/elasticsearch/index_management/indices?_g=()')
        time.sleep(15)


    @classmethod
    def searchAll(self, indx=None):
        # ===============
        # 데이터 조회 [전체]
        # ===============
        res = self.es.search(
            index = "searchnews", doc_type = "_doc",
            body = {
                "query":{"match_all":{}}
            }
        )
        print (json.dumps(res, ensure_ascii=False, indent=4))


instance=Ela()
#instance.createIndex() #인덱스 생성 완료 후, 주석처리할 것.
instance.allIndex()
#instance.collecting()
time.sleep(5)
#instance.dataInsert()
instance.searchAll()

#time.sleep(5)
instance.ExecuteCheck()

instance.srvHealthCheck()





