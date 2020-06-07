import pandas as pd
from tqdm import tqdm_notebook
import numpy as np
import statistics as stat
from scipy import stats
import multiprocessing as mp
from function import test_function
lone_pine = pd.read_csv('lone_pine.csv')
crsp_data = pd.read_csv('StockPriceMonthly.csv', encoding='utf8', low_memory=False)

lone_pine.drop(['% Port', '% OS', 'Hist', 'Change', 'Change.1'], axis=1, inplace=True)

lone_pine['Ticker'] = lone_pine['Ticker'].astype('str')
lone_pine['Class'] = lone_pine['Class'].astype('str')
def ab(aa):
    if '.' in aa:
        return aa.split('.')[0]
    if '/' in aa:
        return aa.split('/')[0]
    else:
        return aa
lone_pine['Ticker'] = lone_pine['Ticker'].apply(ab)

def abc(aa):
    aa = aa.split('/')
    year = aa[2]
    month = aa[0]
    day = aa[1]
    return year+month
lone_pine['Date'] = lone_pine['Date'].apply(abc)

lone_pine = lone_pine[lone_pine['Class']!='PUT'].loc[lone_pine['Class']!='CALL']
lone_pine = lone_pine[lone_pine['Class']!='put'].loc[lone_pine['Class']!='call']
lone_pine = lone_pine[lone_pine['Class']!='Put'].loc[lone_pine['Class']!='Call']

def class_3str(df_col):
    df_col = df_col.replace(' ', '')
    df_col = df_col[0:3]
    df_col = df_col.upper()
    return df_col
lone_pine['Class'] = lone_pine['Class'].apply(class_3str)

lone_pine_not = lone_pine[lone_pine['Ticker']!='nan']
lone_pine_is = lone_pine[lone_pine['Ticker']=='nan']

lone_pine_not.drop('Name', axis=1, inplace=True)
lone_pine_not = lone_pine_not.groupby(['Ticker','Class','Date'])[['Shares', 'Value']].agg('sum').reset_index().copy()

lone_pine_is.loc[:, 'Ticker'] = lone_pine_is['Name']
lone_pine_is.drop('Name', axis=1, inplace=True)
lone_pine_is = lone_pine_is.groupby(['Ticker','Class','Date'])[['Shares', 'Value']].agg('sum').reset_index().copy()

date = sorted(list(set(lone_pine_not['Date'])))

new_lone_pine = pd.DataFrame()

for num in range(len(date)-1):
    first_date = date[num]
    second_date = date[num+1]
    first_df = lone_pine_not[lone_pine_not['Date']==first_date]
    second_df = lone_pine_not[lone_pine_not['Date']==second_date]
    first_df = first_df.drop(['Date'],axis=1)
    first_df.columns = ['Ticker', 'Class', 'Last_Shares', 'Last_Value']
    merge_df = second_df.merge(first_df, on=['Ticker','Class'], how='left')
    if len(merge_df) != len(second_df):
        print(second_date)
    new_lone_pine = pd.concat([new_lone_pine, merge_df], ignore_index=True)

new_lone_pine_is = pd.DataFrame()

for num in range(len(date)-1):
    first_date = date[num]
    second_date = date[num+1]
    first_df = lone_pine_is[lone_pine_is['Date']==first_date]
    second_df = lone_pine_is[lone_pine_is['Date']==second_date]
    first_df = first_df.drop(['Date'],axis=1)
    first_df.columns = ['Ticker', 'Class', 'Last_Shares', 'Last_Value']
    merge_df = second_df.merge(first_df, on=['Ticker','Class'], how='left')
    if len(merge_df) != len(second_df):
        print(second_date)
    new_lone_pine_is = pd.concat([new_lone_pine_is, merge_df], ignore_index=True, sort=False)

new_lone_pine = pd.concat([new_lone_pine, lone_pine_not[lone_pine_not['Date']==date[0]]], ignore_index=True, sort=False)
new_lone_pine_is = pd.concat([new_lone_pine_is, lone_pine_is[lone_pine_is['Date']==date[0]]],ignore_index=True, sort=False)
lone_pine_a = pd.concat([new_lone_pine, new_lone_pine_is], ignore_index=True, sort=False)

lone_pine_a = lone_pine_a.replace(np.nan, 0)

lone_pine_a.loc[:, 'Shares_Change'] = lone_pine_a['Shares'] - lone_pine_a['Last_Shares']
lone_pine_a.loc[:, 'Value_Change'] = lone_pine_a['Value'] - lone_pine_a['Last_Value']

lone_pine_a.drop(['Last_Shares', 'Last_Value'], axis=1, inplace=True)

add_df = pd.DataFrame(columns=lone_pine_a.columns)
for index in tqdm_notebook(range(len(lone_pine_a))):
    each_data = lone_pine_a.loc[index]
    each_data = list(each_data)
    date = int(each_data[2])
    if date % 4 == 0:
        date = date - 12 + 100
    each_data[2] = str(date+1)
    each_data[5] = 0
    each_data[6] = 0
    add_df.loc[-1] = each_data
    add_df = add_df.reset_index(drop=True)
    date = int(each_data[2])
    each_data[2] = str(date+1)
    each_data[5] = 0
    each_data[6] = 0
    add_df.loc[-1] = each_data
    add_df = add_df.reset_index(drop=True)
lone_pine_a = pd.concat([lone_pine_a, add_df], ignore_index=True, sort=False)

lone_pine_a.drop(lone_pine_a[lone_pine_a['Shares']==0].loc[lone_pine_a['Value']==0].loc[lone_pine_a['Shares_Change']==0].loc[lone_pine_a['Value_Change']==0].index, inplace=True)

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
lone_pine_a['Class'] = lone_pine_a['Class'].apply(class_)

def division(df):
    if df['Shares_Change'] == 0 and df['Shares'] == 0:
        return 0
    elif df['Shares'] == 0 and df['Value'] == 0:
        return df['Value_Change'] / df['Shares_Change']
    else:
        return df['Value'] / df['Shares']
lone_pine_a['price'] = lone_pine_a.apply(division, axis=1)

def abcd(aa):
    return str(aa)[0:6]
crsp_data['date'] = crsp_data['date'].apply(abcd)

crsp_data.columns = ['PERMNO', 'Date', 'Ticker', 'COMNAM', 'Class', 'DIVAMT', 'PRC', 'VOL']

def to_abs(df_col):
    return abs(df_col)
crsp_data_c = crsp_data[['Date', 'Ticker', 'PRC', 'Class']]
crsp_data_c = crsp_data_c[crsp_data_c['Ticker'].isin(list(set(lone_pine_a['Ticker'])))]
crsp_data_c = crsp_data_c[crsp_data_c['PRC'].notnull()].drop_duplicates(keep='first')
crsp_data_c['PRC'] = crsp_data_c['PRC'].apply(to_abs)

if __name__ == '__main__':
    pool_list = [i for i in crsp_data_c.groupby(by='Ticker')]
    pool = mp.Pool(processes=5)
    df_list = pool.map(test_function.last_price, pool_list)

crsp_data_c = pd.concat(df_list)

lone_pine_all = lone_pine_a.merge(crsp_data_c, on=['Date', 'Ticker'], how = 'left')

for ticker in list(set(list(lone_pine_all['Ticker']))):
    if len(lone_pine_a[lone_pine_a['Ticker']==ticker].reset_index(drop=True)) != len(lone_pine_all[lone_pine_all['Ticker']==ticker].reset_index(drop=True)):
        drop_df = lone_pine_all[lone_pine_all['Ticker']==ticker].loc[lone_pine_all['Class_x']!=lone_pine_all['Class_y']]
        lone_pine_all.drop(list(drop_df.index), inplace=True)

error = lone_pine_all[lone_pine_all['PRC'].isnull()]

test = lone_pine_all
new_error = []
for index in tqdm_notebook(error.index):
    each = error.loc[index]
    df = crsp_data[crsp_data['Ticker']==each['Ticker']]
    try:
        if int(each['Date']) > int(list(df['Date'])[-1]):
            test = test.drop(index)
        else:
            new_error.append(list(each))
    except:
        new_error.append(list(each))
lone_pine_all = test

new_error = pd.DataFrame(new_error)
new_error.columns = lone_pine_all.columns
new_error = new_error.sort_values(by='Date').reset_index(drop=True)

df_list = []
for each in tqdm_notebook(new_error.groupby(by='Ticker')):
    ticker, df = each
    df.loc[:, 'PRC'] = df['price']
    df.loc[:, 'Last_PRC'] = df['PRC'].shift()
    df_list.append(df)
new_error = pd.concat(df_list)

lone_pine_all = pd.concat([lone_pine_all[lone_pine_all['PRC'].notnull()], new_error], ignore_index=True, sort=False)

lone_pine_all = lone_pine_all.sort_values(by=['Ticker', 'Date']).reset_index(drop=True)