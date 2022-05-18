import util
import requests
from tqdm import tqdm
import datetime
import pickle
from pathlib import Path

class KrxWebScapper:

    def request_daily_data(self,trdDd:str) -> dict:     # format : 20220514
        url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        params = {'trdDd':trdDd, 'mktId':'ALL', 'bld':"dbms/MDC/STAT/standard/MDCSTAT01501"}
        r = requests.post(url=url, params=params)
        if r.status_code == 200:
            return r.json()
        print(f"status code error : {r.status_code}")

    def check_if_trading_day(self, dataList:list):
        for i in dataList:
            if not i == '-':
                return True
        return False

    def get_the_most_recent_day(self):
        filePath='c:/Users/user/PycharmProjects/InPracticeEmploy/normalPayroll.py'

    def save_data(self, start, end) -> None:
        dataSet = {}
        dayList = get_dayList(start, end)
        for day in tqdm(dayList, bar_format='{l_bar}{bar:30}{r_bar}{bar:-10b}'):
            dailyData = self.get_daily_data(trdDd=day)
            if not self.check_if_trading_day([i['TDD_CLSPRC'] for i in dailyData['OutBlock_1'][:10]]):
                continue
            # util.PickleSaver().save({day:dailyData})
            data = {day:dailyData}
            dataSet.update(data)
        filePath = Path.cwd().joinpath('krx', 'db', 'krxDataSet.pickle')
        with open(filePath, "wb") as fw:
            pickle.dump(dataSet, fw, pickle.HIGHEST_PROTOCOL)

def get_dayList(start, end):
    start = datetime.datetime.strptime(start, "%Y%m%d")
    end = datetime.datetime.strptime(end, "%Y%m%d")
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days+1)]

    dayList = []
    for date in date_generated:
        dayList.append(date.strftime("%Y%m%d"))
    return dayList

if __name__ == '__main__':
    from pathlib import Path
    import pickle
    import pandas as pd
    data = KrxWebScapper().save_data(start = '19950502', end = '19951231')  # 1995년 5월 2일부터 가능
    # data = KrxWebScapper().save_data(start = '20220508', end = '20220514')
    # filePath = Path.cwd().joinpath('krx', 'db', 'krxDataSet.pickle')
    # with open(filePath, "rb") as fr:
    #     dataSet = pickle.load(fr)
    # print(list(dataSet.keys())[-10:])
