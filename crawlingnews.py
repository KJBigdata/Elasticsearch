from elasticapi import *

class Crawling(Els):

    def __init__(self,name):
        self.name=name

    def show_name(self):
        print('크롤링 도메인은',self.name)

    def show_path(self):
        print('Crawling path : ',self.base_url)

    def collecting(self):
        # ===============
        # 데이터 크롤링 [네이버 뉴스]
        # ===============
        print('시작')
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
                print('-----------{} {}번째 Crawling-----------'.format(category,i + 1))
                new_block = each_news.split('href="')[1]
                title = new_block.split("<strong>")[1].split("</strong>")[0]
                title_token=self.indices_client.analyze(
                    body={
                        "analyzer":"standard",
                        "text":title
                    }
                )
                tokens=[title_token['tokens'][c]["token"] for c in range(len(title_token['tokens']))]
                collect_time = str(datetime.utcnow())
                link = new_block.split('"')[0].replace("amp;", "")
                soup2 = BeautifulSoup(urlopen(link).read(), "html.parser")
                #article_body = str(soup2.find_all(attrs={"id": "articleBodyContents"}))
                self.driver.get(link)
                self.driver.implicitly_wait(30)

                # 해당 기사에 대한 총 댓글 수
                try:
                    interest_cnt = int(self.driver.find_element_by_css_selector('span.u_cbox_count').text.replace(',',''))
                except:
                    interest_cnt = int(self.driver.find_element_by_css_selector('em.simplecmt_num').text.replace(',',''))

                print("댓글 수: ",interest_cnt)

                insert_data = {"source": "naver_news",
                               "category": category,
                               "title": title,
                               "title_tokens":tokens,
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