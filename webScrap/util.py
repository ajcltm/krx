import os
from pathlib import Path
import datetime

def get_the_last_day_in_fileBase():
    fileBasePath = Path.cwd().joinpath('krx', 'webScrap', 'db')
    lst = os.listdir(fileBasePath)
    if not lst: 
        return None
    return lst[-1].split('_')[-1].split('.')[0]


def get_day_list(start, end=None):
    start = datetime.datetime.strptime(start, "%Y%m%d")
    if end:
        end = datetime.datetime.strptime(end, "%Y%m%d")
    else:
        end = datetime.datetime.today()
    day_list = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days+1)]
    dayList = []
    for date in day_list:
        dayList.append(date.strftime("%Y%m%d"))
    return dayList