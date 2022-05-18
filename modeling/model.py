from pydantic import BaseModel, validator

class krxData(BaseModel):
    DATE : str
    ISU_SRT_CD: str
    ISU_ABBRV : str
    MKT_NM : str
    SECT_TP_NM : str
    TDD_CLSPRC : int
    FLUC_TP_CD : int
    CMPPREVDD_PRC : int
    FLUC_RT : float
    TDD_OPNPRC : int
    TDD_HGPRC : int
    TDD_LWPRC : int
    ACC_TRDVOL : int
    ACC_TRDVAL : int
    MKTCAP : int
    LIST_SHRS : int
    MKT_ID : str

    @validator('*', pre=True)
    def delet_comma(cls, v):
        return v.replace(',', '')

    @validator('*', pre=True, always=True)
    def transfer_none_value(cls, v):
        if v == '-':
            return 0
        return v
    
    @validator('*', pre=True, always=True)
    def transfer_empty_value(cls, v):
        if v == ' ':
            return 0
        return v

class sqlModel:

    sql =   "\
            DATE TIMESTAMP,\
            ISU_SRT_CD VARCHAR(100),\
            ISU_ABBRV VARCHAR(100),\
            MKT_NM VARCHAR(100),\
            SECT_TP_NM VARCHAR(100),\
            TDD_CLSPRC BIGINT,\
            FLUC_TP_CD BIGINT,\
            CMPPREVDD_PRC BIGINT,\
            FLUC_RT FLOAT,\
            TDD_OPNPRC BIGINT,\
            TDD_HGPRC BIGINT,\
            TDD_LWPRC BIGINT,\
            ACC_TRDVOL BIGINT,\
            ACC_TRDVAL BIGINT,\
            MKTCAP BIGINT,\
            LIST_SHRS BIGINT,\
            MKT_ID VARCHAR(100)\
            "

    def get_sql_model_part(self):
        return self.sql


if __name__ == '__main__':
    import pickle
    from pathlib import Path
    from tqdm import tqdm
    from datetime import datetime
    import os

    fileDir = Path.cwd().joinpath('krx', 'webScrap', 'db')
    fileList = os.listdir(fileDir)
    for fileName in tqdm(fileList):
        filePath = fileDir.joinpath(fileName)
        with open(filePath, 'rb') as fr:
            data = pickle.load(fr)
        lst = data['OutBlock_1']
        date = fileName.split('_')[-1].split('.')[0]
        for i in lst:
            try:
                krxData(**dict({'DATE':date}, **i))
            except:
                print(dict({'DATE':date}, **i))

