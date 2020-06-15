import pandas as pd
import numpy as np
import statistics as stat
from scipy import stats
import multiprocessing as mp


class Pre_Processing:
    @staticmethod
    def ticker_split(aa):
        if '.' in aa:
            return aa.split('.')[0]
        if '/' in aa:
            return aa.split('/')[0]
        else:
            return aa

    @staticmethod
    def year_month(aa):
        aa = aa.split('/')
        year = aa[2]
        month = aa[0]
        day = aa[1]
        return year + month

    @staticmethod
    def class_3str(df_col):
        df_col = df_col.replace(' ', '')
        df_col = df_col[0:3]
        df_col = df_col.upper()
        return df_col

    @staticmethod
    def class_(df_col):
        if df_col == 'CLA':
            return 'A'
        elif df_col == 'CLB':
            return 'B'
        elif df_col == 'CLC':
            return 'C'
        elif df_col == 'CLD':
            return 'D'
        else:
            return df_col

    @staticmethod
    def date_split(aa):
        return str(aa)[0:6]

    @staticmethod
    def to_abs(df_col):
        return abs(df_col)

    def launch(self, f13_data, crsp_data):
        f13_data.drop(['% Port', '% OS', 'Hist', 'Change', 'Change.1'], axis=1, inplace=True)
        f13_data['Ticker'] = f13_data['Ticker'].astype('str')
        f13_data['Class'] = f13_data['Class'].astype('str')
        f13_data['Ticker'] = f13_data['Ticker'].apply(self.ticker_split)
        f13_data['Date'] = f13_data['Date'].apply(self.year_month)
        f13_data = f13_data[f13_data['Class'] != 'PUT'].loc[f13_data['Class'] != 'CALL']
        f13_data = f13_data[f13_data['Class'] != 'put'].loc[f13_data['Class'] != 'call']
        f13_data = f13_data[f13_data['Class'] != 'Put'].loc[f13_data['Class'] != 'Call']
        f13_data['Class'] = f13_data['Class'].apply(self.class_3str)
        f13_data['Class'] = f13_data['Class'].apply(self.class_)
        crsp_data = crsp_data[crsp_data['TICKER'].notnull()]
        crsp_data.loc[:, 'SHRCLS'] = crsp_data['SHRCLS'].replace(np.nan, 0)
        crsp_data = crsp_data.sort_values(['TICKER', 'date', 'PRC'])
        crsp_data = crsp_data.drop('PERMNO', axis=1).drop_duplicates(['date', 'TICKER', 'SHRCLS'], keep='first')
        crsp_data = crsp_data[crsp_data['PRC'].notnull()]
        crsp_data['PRC'] = crsp_data['PRC'].apply(self.to_abs)
        crsp_data['BIDLO'] = crsp_data['BIDLO'].apply(self.to_abs)
        crsp_data['ASKHI'] = crsp_data['ASKHI'].apply(self.to_abs)

        crsp_data['date'] = crsp_data['date'].apply(self.date_split)
        crsp_data.columns = ['Date', 'Ticker', 'Class', 'BIDLO', 'ASKHI', 'PRC', 'VOL', 'SHROUT']
        return f13_data, crsp_data