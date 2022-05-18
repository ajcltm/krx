import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))
from webScrap import apps, dataRequests, fileSaver
sys.path.append(str(Path.cwd().joinpath('krx')))
from webScrap import util

from tqdm import tqdm
from pathlib import Path

class KrxScraper:

    def get_scraper(self, trdDd:str) -> apps.JsonDataScraper:
        r = self.get_requester(trdDd)
        fs =self.get_file_saver(trdDd)
        return apps.JsonDataScraper(requester=r, fileSaver=fs)

    def get_requester(self, trdDd):
        url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        params = {
            'trdDd': trdDd, 
            'mktId': 'ALL',
            'bld' : "dbms/MDC/STAT/standard/MDCSTAT01501"
            }
        return dataRequests.PostRequester(url, dataForms=params)

    def get_file_saver(self, trdDd):
        filePath = Path.cwd().joinpath('krx', 'webScrap', 'db', f'krxDataSet_{trdDd}.pickle')
        return fileSaver.PickleSaver(filePath)

class AKrxScraper:

    def execute(self, end=None):
        ks = KrxScraper()
        theLastDay = util.get_the_last_day_in_fileBase()
        if not theLastDay:
            theLastDay = '20220507'
        dayLst = util.get_day_list(start=theLastDay, end=end)

        for day in tqdm(dayLst):
            ks.get_scraper(day).execute()


def main():
    AKrxScraper().execute()


if __name__ == '__main__':
    main()
