import pandas as pd
import numpy as np

beta = pd.read_csv(r'CTBC-PROJECT/lone pine/beta.csv')
beta = beta[['DATE', 'RET', 'b_mkt', 'alpha', 'R2', 'TICKER']].drop_duplicates(keep='first', inplace=False)
beta.columns = ['date', 'RET', 'b_mkt', 'alpha', 'R2', 'ticker']
asset = pd.read_csv(r'CTBC-PROJECT/lone pine/asset and book value.csv')
asset = asset[['fyear', 'indfmt', 'tic', 'at', 'bkvlps', 'costat']].drop_duplicates(keep='first', inplace=False)
asset.columns = ['date', 'indfmt', 'ticker', 'at', 'bkvlps', 'costat']
asset = asset[asset['indfmt']!='FS'].loc[asset['ticker'].notnull()].reset_index(drop=True)
stock = pd.read_csv(r'CTBC-PROJECT/lone pine/StockPriceMonthly.csv')
stock = stock[['date', 'TICKER', 'PRC', 'SHRCLS']].drop_duplicates(keep='first', inplace=False)
stock = stock[stock['PRC'].notnull()].reset_index(drop=True)
stock.columns = ['date', 'ticker', 'price', 'class']

def abs_price(df_col):
    return abs(df_col)
stock['price'] = stock['price'].apply(abs_price)

def date_col(df_col):
    df_col = str(df_col)
    return df_col[0:6]

beta['date'] = beta['date'].apply(date_col)

asset['date'] = (asset['date']*100+12).astype('str')

stock['date'] = stock['date'].apply(date_col)

bp = asset.merge(stock, on=['date', 'ticker'], how='left')

bp['book/price'] = bp['bkvlps'] / bp['price']

bp = bp[bp['book/price'].notnull()].reset_index(drop=True)

bp['class'] = bp['class'].replace(np.nan, 0)

last_date = None
last_ticker = None
last_class = None
error_index = []
for index in tqdm_notebook(bp.index):
    date = bp.loc[index]['date']
    ticker = bp.loc[index]['ticker']
    class_ = bp.loc[index]['class']
    if last_date == date and last_ticker == ticker and last_class == class_:
        error_index.append(index)
        error_index.append(index-1)
    last_date = date
    last_ticker = ticker
    last_class = class_

for index in error_index:
    bp = bp.drop(index)

bp.loc[:, 'last_at'] = bp.groupby(['ticker'])['at'].shift()
bp.loc[:, '2last_at'] = bp.groupby(['ticker'])['last_at'].shift()
bp.loc[:, 'last-2last'] = bp['last_at'] - bp['2last_at']
bp.loc[:, 'investment'] = bp['last-2last'] / bp['2last_at']
bp = bp[bp['book/price'].notnull()].reset_index(drop=True)
bp = bp[bp['investment'].notnull()].reset_index(drop=True)

bp = bp.merge(beta[['ticker', 'date', 'b_mkt']], on=['ticker', 'date'])

bp = bp[bp['b_mkt'].notnull()]

profolio = bp[['date', 'ticker', 'bkvlps', 'price', 'book/price', 'investment', 'b_mkt']]
all_profolio = pd.DataFrame()
for date, df in tqdm_notebook(profolio.groupby(['date'])):
    length = len(df)
    len30 = int(length*0.3)
    df = df.sort_values('book/price').reset_index(drop=True)
    hml_list = [1] * len30 + [0] * (length-len30)
    df.loc[:, 'hml'] = hml_list
    df = df.sort_values('investment').reset_index(drop=True)
    cma_list = [1] * len30 + [0] * (length-len30)
    df.loc[:, 'cma'] = cma_list
    all_profolio = pd.concat([all_profolio, df], ignore_index=True, sort=False)

all_profolio