import sys
from pathlib import Path
sys.path.append(str(Path.cwd().joinpath('krx')))
from modeling import model, util
from datetime import datetime
import pandas as pd
from tqdm import tqdm

class FileToParquet :

    def execute(self):
        fileDir = Path.cwd().joinpath('krx', 'webScrap', 'db')
        fileLst = util.get_file_lst(fileDir)
        dfs = []
        for fileName in tqdm(fileLst):
            filePath = fileDir.joinpath(fileName)
            date = util.get_dateString_from_fileName(fileName)
            df = self.get_df(filePath, date)
            dfs.append(df)
        return pd.concat(dfs, axis=1)

    def get_df(self, file_path, date):
        data = util.get_data(file_path)
        date = datetime.strptime(date, '%Y%m%d').strftime(format='%Y-%m-%d')
        data_lst = data['OutBlock_1']
        if not util.check_bussiness_day(data_lst):
            return
        data_lst_ = [model.krxData(**dict({'DATE':date}, **i)) for i in data_lst]
        return pd.DataFrame(data_lst_)

def main():
    df = FileToParquet().execute()
    df.to_parquet(Path.cwd().joinpath('krx', 'modeling', 'db'))


if __name__ == '__main__':

    df = main()
    print(df.head())

