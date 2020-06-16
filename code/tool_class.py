import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics


class Tool:
    def __init__(self, f13_data, price_data, industry, asset, date):
        self.f13_data = f13_data
        self.price_data = price_data
        self.industry = industry
        self.asset = asset
        self.date = str(date)
        
    @staticmethod
    def date_col2(df_col):
        df_col = str(df_col)
        return df_col[0:4]
    
    @staticmethod
    def crawler(url):
        from crawler import crawler
        f13_data = crawler(url)
        return f13_data
    
    @staticmethod
    def pre_processing(f13_data, crsp_data):
        import pre_processing as pp
        f13_data_all = pp.Pre_Processing().launch(f13_data, crsp_data)
        return f13_data_all
    
    @staticmethod
    def industry_sort(data, date):
        from industry import indus_sort
        season_date_list,gsector_sort, ggroup_sort = indus_sort(data, date)
        return season_date_list,gsector_sort, ggroup_sort
    
    @staticmethod
    def company_combine(industry, asset, stock):
        import company_combine as cc
        company = cc.Company_Combine().launch(industry, asset, stock)
        return company
    
    @staticmethod
    def industry_filter(company, gsector_sort):
        company = company[company['gsector'].isin(list(gsector_sort.keys()))].reset_index(drop=True)
        return company
        
    @staticmethod
    def cma_hml(unbuy_tic):
        unbuy_tic = unbuy_tic.sort_values('book/price').reset_index(drop=True)
        unbuy_tic = unbuy_tic[:int(len(unbuy_tic)/2)]
        unbuy_tic = unbuy_tic.sort_values('investment', ascending=True).reset_index(drop=True)
        unbuy_tic = unbuy_tic[:int(len(unbuy_tic)/2)]
        return unbuy_tic
    
    def training_set(self):
        date = self.date
        if int(date[4:]) == 1:
            last_date = str(int(date)-100+11)
        else:
            last_date = str(int(date)-1)
        year = date[0:4]
        if int(date[4:]) == 1 or int(date[4:]) == 2 or int(date[4:]) == 3:
            seanson_date = str(int(date[0:4])-1) + '12'
        elif int(date[4:]) == 4 or int(date[4:]) == 5 or int(date[4:]) == 6:
            seanson_date = date[0:4] + '03'
        elif int(date[4:]) == 7 or int(date[4:]) == 8 or int(date[4:]) == 9:
            seanson_date = date[0:4] + '06'
        elif int(date[4:]) == 10 or int(date[4:]) == 11 or int(date[4:]) == 12:
            seanson_date = date[0:4] + '09'
        
        self.f13_data, self.price_data = self.pre_processing(self.f13_data, self.price_data)
        self.industry = self.industry[['tic', 'gsector', 'ggroup']]
        self.industry.columns = ['ticker', 'gsector', 'ggroup']
        
        f13_data_indus = self.f13_data.merge(self.industry, left_on='Ticker', right_on='ticker', how='left')
        season_date_list,gsector_sort, ggroup_sort = Tool.industry_sort(f13_data_indus, seanson_date)
        
        self.asset['fyear'] = self.asset['fyear'].astype('str')
        stock = self.price_data[['Date', 'Ticker', 'PRC', 'Class']]
        stock = stock[stock['Date']==last_date].reset_index(drop=True)
        stock['year'] = stock['Date'].apply(self.date_col2)
        
        all_company = self.company_combine(self.industry, self.asset, stock)
        
        all_company = self.industry_filter(all_company, gsector_sort)
        
        ticker_list = list(set(self.f13_data[self.f13_data['Date'].isin(season_date_list)]['Ticker']))
        
        buy_tic = all_company[all_company['ticker'].isin(ticker_list)]
        buy_tic.loc[:, 'buy'] = 1
        unbuy_tic = all_company[~all_company['ticker'].isin(ticker_list)]
        unbuy_tic = self.cma_hml(unbuy_tic)
        unbuy_tic.loc[:, 'buy'] = 0
        buy_unbuy = pd.concat([buy_tic, unbuy_tic], ignore_index=True, sort=False)
        
        all_date = sorted(list(set(self.price_data['Date'].astype('int'))))
        index = all_date.index(int(date))
        start = index - 12
        end = index
        if start < 0:
            start = 0
        last_12month = all_date[start:end]
        last_12month = [str(i) for i in last_12month]
        last_year_price = self.price_data[self.price_data['Date'].isin(last_12month)]
        last_year_price = last_year_price.merge(buy_unbuy[['ticker', 'class', 'buy']],\
                                                left_on=['Ticker', 'Class'], right_on=['ticker', 'class'])
        final = pd.DataFrame(columns=['ticker', 'class', 'volatility', 'liquidity', '52 week high', 'momentum'])
        
        for group, df in last_year_price.groupby(['Ticker', 'Class']):
            ticker, cls = group
            max_price = max(df['ASKHI'])
            min_price = min(df['BIDLO'])
            week_high = max_price / min_price
            df = df.sort_values('Date').reset_index(drop=True)
            df_len = len(df)
            mom = np.mean(df[df_len-2:df_len]['PRC']) / np.mean(df['PRC'])
            df = df[df['Date']==last_date].reset_index(drop=True)
            vol = df['VOL'][0]
            liq = vol / df['SHROUT'][0]
            final.loc[-1] = [ticker, cls, vol, liq, week_high, mom]
            final = final.reset_index(drop=True)
        feature_data = buy_unbuy[['ticker', 'class', 'buy']].merge(final, on=['ticker', 'class'])
        return feature_data
    
    def testing_set(self):
        last_date = self.date
        if int(last_date[4:]) == 12:
            date = str(int(last_date)+100-11)
        else:
            date = str(int(last_date)+1)
        year = date[0:4]
        if int(date[4:]) == 1 or int(date[4:]) == 2 or int(date[4:]) == 3:
            seanson_date = str(int(date[0:4])-1) + '12'
        elif int(date[4:]) == 4 or int(date[4:]) == 5 or int(date[4:]) == 6:
            seanson_date = date[0:4] + '03'
        elif int(date[4:]) == 7 or int(date[4:]) == 8 or int(date[4:]) == 9:
            seanson_date = date[0:4] + '06'
        elif int(date[4:]) == 10 or int(date[4:]) == 11 or int(date[4:]) == 12:
            seanson_date = date[0:4] + '09'
        
        f13_data_indus = self.f13_data.merge(self.industry, left_on='Ticker', right_on='ticker', how='left')
        season_date_list,gsector_sort, ggroup_sort = Tool.industry_sort(f13_data_indus, seanson_date)
        
        stock = self.price_data[['Date', 'Ticker', 'PRC', 'Class']]
        stock = stock[stock['Date']==last_date].reset_index(drop=True)
        stock['year'] = stock['Date'].apply(self.date_col2)
        
        all_company = self.company_combine(self.industry, self.asset, stock)
        
        all_company = self.industry_filter(all_company, gsector_sort)
        
        ticker_list = list(set(self.f13_data[self.f13_data['Date'].isin(season_date_list)]['Ticker']))
        
        buy_tic = all_company[all_company['ticker'].isin(ticker_list)]
        unbuy_tic = all_company[~all_company['ticker'].isin(ticker_list)]
        unbuy_tic = self.cma_hml(unbuy_tic)
        buy_unbuy = pd.concat([buy_tic, unbuy_tic], ignore_index=True, sort=False)
        
        all_date = sorted(list(set(self.price_data['Date'].astype('int'))))
        final_date = all_date[-1]
        if str(final_date)[4:] == '12':
            all_date.append(final_date + 100 - 11)
        else:
            all_date.append(final_date + 1)
        index = all_date.index(int(date))
        start = index - 12
        end = index
        if start < 0:
            start = 0
        last_12month = all_date[start:end]
        last_12month = [str(i) for i in last_12month]
        last_year_price = self.price_data[self.price_data['Date'].isin(last_12month)]
        last_year_price = last_year_price.merge(buy_unbuy[['ticker', 'class']],\
                            left_on=['Ticker', 'Class'], right_on=['ticker', 'class'])
        final = pd.DataFrame(columns=['ticker', 'class', 'volatility', 'liquidity', '52 week high', 'momentum'])
        
        for group, df in last_year_price.groupby(['Ticker', 'Class']):
            ticker, cls = group
            max_price = max(df['ASKHI'])
            min_price = min(df['BIDLO'])
            week_high = max_price / min_price
            df = df.sort_values('Date').reset_index(drop=True)
            df_len = len(df)
            mom = np.mean(df[df_len-2:df_len]['PRC']) / np.mean(df['PRC'])
            df = df[df['Date']==last_date].reset_index(drop=True)
            vol = df['VOL'][0]
            liq = vol / df['SHROUT'][0]
            final.loc[-1] = [ticker, cls, vol, liq, week_high, mom]
            final = final.reset_index(drop=True)
        return final
        
    
    def svc_model(self, x, y):
        score_sum = 0
        for i in range(10):
            x_train, x_test, y_train, y_test = train_test_split(x, y.ravel(), test_size=0.25)
            clf = SVC(kernel='rbf')
            clf.fit(x_train, y_train)
            y_pred = clf.predict(x_test)
            score = accuracy_score(y_test, y_pred)
            score_sum = score_sum + score
        score_mean = score_sum / 10
        if score_mean >= 0.75:
            clf = SVC(kernel='rbf')
            clf.fit(x, y.ravel())
            return clf
        else:
            return False
    
    def rft_model(self, x, y):
        score_sum = 0
        for i in range(10):
            x_train, x_test, y_train, y_test = train_test_split(x, y.ravel(), test_size=0.25)
            model = RandomForestClassifier(n_estimators=1000)
            model.fit(x_train, y_train)
            y_pred = model.predict(x_test)
            score = accuracy_score(y_test, y_pred)
            score_sum = score_sum + score
        score_mean = score_sum / 10
        if score_mean >= 0.75:
            model = RandomForestClassifier(n_estimators=1000)
            model.fit(x, y.ravel())
            return model
        else:
            return False