import os
import pickle
from datetime import datetime

def get_file_lst(fileDir):
    return os.listdir(fileDir)

def get_dateString_from_fileName(fileName):
    return fileName.split('_')[-1].split('.')[0]

def get_data(file_path):
    with open(file_path, 'rb') as fr:
        data = pickle.load(fr)
    return data

def check_bussiness_day(data):
    dataList = data[:10]
    for data in dataList:
        if not data['TDD_CLSPRC'] == '-':
            return True
    return False

def get_the_last_day_in_db(excuter):
    sql = 'select DATE from dailyData order by DATE desc limit 1'
    try:
        excuter.execute(sql)
        for i in excuter.c.fetchall():
            return i[0].strftime(format='%Y%m%d')
    except:
        return None
