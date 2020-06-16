import pandas as pd
import numpy as np
import tool_class

class test_fun:
    @staticmethod
    def fun(index):
        f13_data = pd.read_csv('lone_pine.csv')
        price_data = pd.read_csv('liquidity+highlow+class.csv', encoding='utf8', low_memory=False)
        industry = pd.read_csv('industry2.csv')
        asset = pd.read_csv('asset and book value.csv')
        our_revenue = []
        lone_pine_revenue = []
        f13_data1 = f13_data.copy()
        price_data1 = price_data.copy()
        industry1 = industry.copy()
        asset1 = asset.copy()
        a_date = []
        for i in range(200501, 201913, 1):
            i = str(i)
            if 1 <= int(i[4:]) <= 12:
                a_date.append(i)
        date = str(a_date[index])
        print(date)
        if int(date[4:]) == 1 or int(date[4:]) == 11:
            seanson_date = str(int(date[0:4])-1) + '12'
        elif int(date[4:]) == 4 or int(date[4:]) == 2:
            seanson_date = date[0:4] + '03'
        elif int(date[4:]) == 7 or int(date[4:]) == 5:
            seanson_date = date[0:4] + '06'
        elif int(date[4:]) == 10 or int(date[4:]) == 8:
            seanson_date = date[0:4] + '09'
        else:
            seanson_date = date
        tool = tool_class.Tool(f13_data1, price_data1, industry1, asset1, date)

        feature_data = tool.training_set()

        testing_data = tool.testing_set()

        last_buy_count = len(feature_data[feature_data['buy']==1])
        not_buy_count = len(feature_data[feature_data['buy']==0])
        start = last_buy_count+1
        try:
            while start + last_buy_count * 4 < len(feature_data):
                t_data = pd.concat([feature_data[:last_buy_count],feature_data[start:start+last_buy_count*2]])
                x = t_data[['volatility', 'liquidity', '52 week high', 'momentum']].values
                y = t_data[['buy']].values
                x_test = testing_data[['volatility', 'liquidity', '52 week high', 'momentum']].values
                svc_clf = tool.svc_model(x, y)
                if svc_clf is not False:
                    y_pred = svc_clf.predict(x_test)
                    testing_data.loc[:, 'svc'] = y_pred
                    testing_data = testing_data[testing_data['svc']==1]
                x_test = testing_data[['volatility', 'liquidity', '52 week high', 'momentum']].values
                rft_clf = tool.rft_model(x, y)
                if rft_clf is not False:
                    y_pred = rft_clf.predict(x_test)
                    testing_data.loc[:, 'rtf'] = y_pred
                    testing_data = testing_data[testing_data['rtf']==1]
                start = start+last_buy_count*2

            t_data = pd.concat([feature_data[:last_buy_count],feature_data[start:]])
            x = t_data[['volatility', 'liquidity', '52 week high', 'momentum']].values
            y = t_data[['buy']].values
            x_test = testing_data[['volatility', 'liquidity', '52 week high', 'momentum']].values
            svc_clf = tool.svc_model(x, y)
            if svc_clf is not False:
                y_pred = svc_clf.predict(x_test)
                testing_data.loc[:, 'svc'] = y_pred
                testing_data = testing_data[testing_data['svc']==1]
            x_test = testing_data[['volatility', 'liquidity', '52 week high', 'momentum']].values
            rft_clf = tool.rft_model(x, y)
            if rft_clf is not False:
                y_pred = rft_clf.predict(x_test)
                testing_data.loc[:, 'rtf'] = y_pred
                testing_data = testing_data[testing_data['rtf']==1]

            test = tool.price_data.merge(testing_data[['ticker', 'class']], left_on=['Ticker', 'Class'], right_on=['ticker', 'class'])
            our_revenue.append(sum(test[test['Date']==str(a_date[index+1])]['PRC'])/sum(test[test['Date']==date]['PRC']))


            def abcd(df_col):
                if df_col == 'A':
                    return df_col
                else:
                    return 0
            tdf = tool.f13_data[tool.f13_data['Date']==seanson_date][['Ticker', 'Class']]
            tdf['Class'] = tdf['Class'].apply(abcd)
            test1 = tool.price_data.merge(tdf, on=['Ticker', 'Class'])
            lone_pine_revenue.append(sum(list(test1[test1['Date']==str(a_date[index+1])]['PRC'])) / sum(list(test1[test1['Date']==date]['PRC'])))
            return our_revenue+lone_pine_revenue
        except:
            return date