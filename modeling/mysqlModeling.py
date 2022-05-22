import sys
parentPath='c:/Users/ajcltm/PycharmProjects' # parent 경로
sys.path.append(parentPath) # 경로 추가
from Isql import connect, execute, util
parentPath='c:/Users/ajcltm/PycharmProjects/krx' # parent 경로
sys.path.append(parentPath) # 경로 추가
from modeling import model
from modeling import util as ut

from pathlib import Path
from tqdm import tqdm
from datetime import datetime

class FileToSql :

    def __init__(self, sqlExcuter):
        self.sqlExcuter = sqlExcuter

    def execute(self, file_path, date):
        data = ut.get_data(file_path)
        date = datetime.strptime(date, '%Y%m%d').strftime(format='%Y-%m-%d')
        data_lst = data['OutBlock_1']
        if not ut.check_bussiness_day(data_lst):
            return
        data_lst_ = [model.krxData(**dict({'DATE':date}, **i)) for i in data_lst]
        data_lst__ = [i.dict() for i in data_lst_]
        values_parts = util.InsertFormatter().get_values_parts(data_lst__)
        sql = f"insert into dailyData values {values_parts}"
        self.sqlExcuter.execute(sql)

def main():
    fileDir = Path.cwd().joinpath('krx', 'webScrap', 'db')
    fileLst = ut.get_file_lst(fileDir)
    db = connect.get_connector('mysql', 'krxData')
    db_executer = execute.Excuter(db)
    model_part = model.sqlModel().get_sql_model_part()
    sql = f'CREATE TABLE IF NOT EXISTS dailyData({model_part})'
    theLastDay = ut.get_the_last_day_in_db(db_executer)
    if not theLastDay:
        idx=0
    else:
        idx = fileLst.index(f'krxDataSet_{theLastDay}.pickle')+1
    db_executer.execute(sql)
    fts = FileToSql(db_executer)
    for fileName in tqdm(fileLst[idx:]):
        date = ut.get_dateString_from_fileName(fileName)
        filePath = fileDir.joinpath(fileName)
        fts.execute(filePath, date)
    db.commit()

if __name__ == '__main__':
    main()


    