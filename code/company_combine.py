import pandas as pd
import numpy as np

class Company_Combine:
    @staticmethod
    def abs_price(df_col):
        return abs(df_col)

    def launch(self, industry, asset, stock):
        # beta = beta[['DATE', 'RET', 'b_mkt', 'alpha', 'R2', 'TICKER', 'year']].drop_duplicates(keep='first', inplace=False)
        # beta.columns = ['date', 'RET', 'b_mkt', 'alpha', 'R2', 'ticker', 'year']
        asset = asset[['fyear', 'indfmt', 'tic', 'at', 'bkvlps', 'costat']].drop_duplicates(keep='first', inplace=False)
        asset.columns = ['year', 'indfmt', 'ticker', 'at', 'bkvlps', 'costat']
        asset = asset[asset['indfmt']!='FS'].loc[asset['ticker'].notnull()].reset_index(drop=True)
        asset['year'] = asset['year'].astype('str')
        # stock = stock[['date', 'TICKER', 'PRC', 'SHRCLS', 'year']].drop_duplicates(keep='first', inplace=False)
        stock = stock[stock['PRC'].notnull()].reset_index(drop=True)
        stock.columns = ['date', 'ticker', 'price', 'class', 'year']
        stock['price'] = stock['price'].apply(self.abs_price)
        asset.loc[:, 'last_at'] = asset.groupby(['ticker'])['at'].shift()
        asset.loc[:, '2last_at'] = asset.groupby(['ticker'])['last_at'].shift()
        asset.loc[:, 'last-2last'] = asset['last_at'] - asset['2last_at']
        asset.loc[:, 'investment'] = asset['last-2last'] / asset['2last_at']
        asset = asset[asset['investment'].notnull()].reset_index(drop=True)
        company = stock.merge(asset, on=['year', 'ticker'], how='left')
        company['book/price'] = company['bkvlps'] / company['price']
        company = company[company['book/price'].notnull()].reset_index(drop=True)
        company['class'] = company['class'].replace(np.nan, 0)
        company = company.drop_duplicates(keep='first', inplace=False)
        # company = company.merge(beta[['ticker', 'date', 'b_mkt']], on=['ticker', 'date'], how='left')
        # company = company[company['b_mkt'].notnull()]
        company = company.merge(industry, on='ticker', how='left')
        # company.to_csv('all_company.csv', index=False)
        return company