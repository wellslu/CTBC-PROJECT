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
            return '0'

    @staticmethod
    def date_split(aa):
        return str(aa)[0:6]

    @staticmethod
    def to_abs(df_col):
        return abs(df_col)

    def launch(self, f13_data, crsp_data):
        a_date = []
        for i in range(200501, 201913, 1):
            i = str(i)
            if 1 <= int(i[4:]) <= 12:
                a_date.append(i)
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
        #         f13_data_not = f13_data[f13_data['Ticker']!='nan']
        #         f13_data_is = f13_data[f13_data['Ticker']=='nan']
        #         f13_data_not.drop('Name', axis=1, inplace=True)
        #         f13_data_not = f13_data_not.groupby(['Ticker','Class','Date'])[['Shares', 'Value']].agg('sum').reset_index().copy()
        #         f13_data_is.loc[:, 'Ticker'] = f13_data_is['Name']
        #         f13_data_is.drop('Name', axis=1, inplace=True)
        #         f13_data_is = f13_data_is.groupby(['Ticker','Class','Date'])[['Shares', 'Value']].agg('sum').reset_index().copy()
        #         date = sorted(list(set(f13_data_not['Date'])))
        #         new_f13_data = pd.DataFrame()
        #         for num in range(len(date)-1):
        #             first_date = date[num]
        #             second_date = date[num+1]
        #             first_df = f13_data_not[f13_data_not['Date']==first_date]
        #             second_df = f13_data_not[f13_data_not['Date']==second_date]
        #             first_df = first_df.drop(['Date'],axis=1)
        #             first_df.columns = ['Ticker', 'Class', 'Last_Shares', 'Last_Value']
        #             merge_df = second_df.merge(first_df, on=['Ticker','Class'], how='left')
        #             if len(merge_df) != len(second_df):
        #                 print(second_date)
        #             new_f13_data = pd.concat([new_f13_data, merge_df], ignore_index=True)
        #         new_f13_data_is = pd.DataFrame()
        #         for num in range(len(date)-1):
        #             first_date = date[num]
        #             second_date = date[num+1]
        #             first_df = f13_data_is[f13_data_is['Date']==first_date]
        #             second_df = f13_data_is[f13_data_is['Date']==second_date]
        #             first_df = first_df.drop(['Date'],axis=1)
        #             first_df.columns = ['Ticker', 'Class', 'Last_Shares', 'Last_Value']
        #             merge_df = second_df.merge(first_df, on=['Ticker','Class'], how='left')
        #             if len(merge_df) != len(second_df):
        #                 print(second_date)
        #             new_f13_data_is = pd.concat([new_f13_data_is, merge_df], ignore_index=True, sort=False)
        #         new_f13_data = pd.concat([new_f13_data, f13_data_not[f13_data_not['Date']==date[0]]], ignore_index=True, sort=False)
        #         new_f13_data_is = pd.concat([new_f13_data_is, f13_data_is[f13_data_is['Date']==date[0]]],ignore_index=True, sort=False)
        #         f13_data_a = pd.concat([new_f13_data, new_f13_data_is], ignore_index=True, sort=False)
        #         f13_data_a = f13_data_a.replace(np.nan, 0)
        #         f13_data_a.loc[:, 'Shares_Change'] = f13_data_a['Shares'] - f13_data_a['Last_Shares']
        #         f13_data_a.loc[:, 'Value_Change'] = f13_data_a['Value'] - f13_data_a['Last_Value']
        #         f13_data_a.drop(['Last_Shares', 'Last_Value'], axis=1, inplace=True)
        #         add_df = pd.DataFrame(columns=f13_data_a.columns)
        #         for index in range(len(f13_data_a)):
        #             each_data = f13_data_a.loc[index]
        #             each_data = list(each_data)
        #             date = int(each_data[2])
        #             if date % 4 == 0:
        #                 date = date - 12 + 100
        #             each_data[2] = str(date+1)
        #             each_data[5] = 0
        #             each_data[6] = 0
        #             add_df.loc[-1] = each_data
        #             add_df = add_df.reset_index(drop=True)
        #             date = int(each_data[2])
        #             each_data[2] = str(date+1)
        #             each_data[5] = 0
        #             each_data[6] = 0
        #             add_df.loc[-1] = each_data
        #             add_df = add_df.reset_index(drop=True)
        #         f13_data_a = pd.concat([f13_data_a, add_df], ignore_index=True, sort=False)
        #         f13_data_a.drop(f13_data_a[f13_data_a['Shares']==0].loc[f13_data_a['Value']==0].loc[f13_data_a['Shares_Change']==0].loc[f13_data_a['Value_Change']==0].index, inplace=True)
        #         f13_data_a['Class'] = f13_data_a['Class'].apply(self.class_)
        #         f13_data_a['price'] = f13_data_a.apply(self.division, axis=1)

        crsp_data = crsp_data[crsp_data['TICKER'].notnull()]
        crsp_data.loc[:, 'SHRCLS'] = crsp_data['SHRCLS'].replace(np.nan, '0')
        crsp_data = crsp_data.sort_values(['TICKER', 'date', 'PRC'])
        crsp_data = crsp_data.drop('PERMNO', axis=1).drop_duplicates(['date', 'TICKER', 'SHRCLS'], keep='first')
        crsp_data = crsp_data[crsp_data['PRC'].notnull()]
        crsp_data['PRC'] = crsp_data['PRC'].apply(self.to_abs)
        crsp_data['BIDLO'] = crsp_data['BIDLO'].apply(self.to_abs)
        crsp_data['ASKHI'] = crsp_data['ASKHI'].apply(self.to_abs)

        crsp_data['date'] = crsp_data['date'].apply(self.date_split)
        crsp_data.columns = ['Date', 'Ticker', 'Class', 'BIDLO', 'ASKHI', 'PRC', 'VOL', 'SHROUT']
        # crsp_data_c = crsp_data[['Date', 'Ticker', 'PRC', 'Class']]
        # crsp_data_c = crsp_data_c[crsp_data_c['Ticker'].isin(list(set(f13_data_a['Ticker'])))]
        # crsp_data_c = crsp_data_c[crsp_data_c['PRC'].notnull()].drop_duplicates(keep='first')
        # crsp_data_c['PRC'] = crsp_data_c['PRC'].apply(self.to_abs)
        #         pool_list = [i for i in crsp_data_c.groupby(by='Ticker')]
        #         pool = mp.Pool(processes=5)
        #         df_list = pool.map(self.last_price, pool_list)
        #         crsp_data_c = pd.concat(df_list)
        #         f13_data_all = f13_data_a.merge(crsp_data_c, on=['Date', 'Ticker'], how = 'left')
        #         for ticker in list(set(list(f13_data_all['Ticker']))):
        #             if len(f13_data_a[f13_data_a['Ticker']==ticker].reset_index(drop=True)) != len(f13_data_all[f13_data_all['Ticker']==ticker].reset_index(drop=True)):
        #                 drop_df = f13_data_all[f13_data_all['Ticker']==ticker].loc[f13_data_all['Class_x']!=f13_data_all['Class_y']]
        #                 f13_data_all.drop(list(drop_df.index), inplace=True)
        #         error = f13_data_all[f13_data_all['PRC'].isnull()]
        #         test = f13_data_all
        #         new_error = []
        #         for index in error.index:
        #             each = error.loc[index]
        #             df = crsp_data[crsp_data['Ticker']==each['Ticker']]
        #             try:
        #                 if int(each['Date']) > int(list(df['Date'])[-1]):
        #                     test = test.drop(index)
        #                 else:
        #                     new_error.append(list(each))
        #             except:
        #                 new_error.append(list(each))
        #         f13_data_all = test
        #         new_error = pd.DataFrame(new_error)
        #         new_error.columns = f13_data_all.columns
        #         new_error = new_error.sort_values(by='Date').reset_index(drop=True)
        #         df_list = []
        #         for ticker, df in new_error.groupby(by='Ticker'):
        #             df.loc[:, 'PRC'] = df['price']
        #             df.loc[:, 'Last_PRC'] = df['PRC'].shift()
        #             df_list.append(df)
        #         new_error = pd.concat(df_list)
        #         f13_data_all = pd.concat([f13_data_all[f13_data_all['PRC'].notnull()], new_error], ignore_index=True, sort=False)
        #         f13_data_all = f13_data_all.sort_values(by=['Ticker', 'Date']).reset_index(drop=True)
        #         f13_data_all.to_csv('lone_pine_all.csv', index=False)
        #         f13_data.to_csv('lone_pine_p.csv', index=False)
        return f13_data, crsp_data