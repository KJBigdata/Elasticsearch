import crawlingnews
import elasticapi

import schedule
import time
from datetime import datetime, timedelta

def job():
    now_date = datetime.now().strftime('%Y-%m-%d')
    now_time = datetime.now().strftime('%H:%M:%S')

    if (now_time>"09:00:00") and (now_time < "23:30:00"):
        news=crawlingnews.Crawling('네이버')
        news.collecting()
         # 9시가 되면  import  한 함수를 실행합니다.
        pyapi=elasticapi.Els()
        pyapi.dataInsert()
        pyapi.searchAll()

        # 성공 했다면 실행 확인을 위해 실행 시간을 출력합니다.

        print('Run - ' + str(now_date) + ' ' + str(now_time))


# 이 스케줄은 30분에 한번씩 체크를 합니다.

schedule.every(30).minutes.do(job)

# 무한루프를 돌면서 스케줄을 유지합니다.

while 1:
    schedule.run_pending()

    time.sleep(1800)
