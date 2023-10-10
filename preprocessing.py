import pandas as pd
import time
import requests
from tqdm import tqdm_notebook
import json
import statistics as stat
import multiprocessing as mp


def into_pool(pool_list):
    try:
        russo_list, start, end, nas = pool_list
        stock_info = Get_Stock_Info(russo_list, start, end, nas)
        result = stock_info.launch()
        return result
    except:
        global error
        russo_list, start, end, nas = pool_list
        error.loc[-1] = russo_list
        error = error.reset_index(drop=True)


def get_nas_data(start, end):
    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-historical-data"

    querystring = {"frequency": "1d", "filter": "history", "period1": start, "period2": end, "symbol": "%5EIXIC"}

    headers = {
        'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com",
        'x-rapidapi-key': "8099cb7d77msh3ebb39dd41af894p154acajsnabc8c78e3d42"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    response = json.loads(response.text)

    for i in response['prices']:
        try:
            i['volume']
        except:
            response['prices'].remove(i)

    return response['prices']


class Get_Stock_Info:
    def __init__(self, russo_list, start, end, nas):
        self.name = russo_list[0]
        if '.' in russo_list[1]:
            self.ticker = russo_list[1].replace('.', '-')
        elif '/' in russo_list[1]:
            self.ticker = russo_list[1].replace('/', '-')
        else:
            self.ticker = russo_list[1]
        self.clas = russo_list[2]
        self.share = russo_list[3]
        self.change = russo_list[4]
        self.value = russo_list[5]
        self.start = start
        self.end = end
        self.nas = nas
        self.stock = None
        self.result = pd.DataFrame({'name': [],
                                    'ticker': [],
                                    'price': [],
                                    'volume': [],
                                    'change': [],
                                    'max_change': [],
                                    'beta': [],
                                    'industry': [],
                                    'dividendRate': []})

    def get_historical_data(self):
        url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-historical-data"

        querystring = {"frequency": "1d", "filter": "history", "period1": self.start, "period2": self.end,
                       "symbol": self.ticker}

        headers = {
            'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com",
            'x-rapidapi-key': "8099cb7d77msh3ebb39dd41af894p154acajsnabc8c78e3d42"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)

        response = json.loads(response.text)

        for i in response['prices']:
            try:
                i['volume']
            except:
                response['prices'].remove(i)

        return response['prices']

    def get_profile(self):
        url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-profile"

        querystring = {"symbol": self.ticker}

        headers = {
            'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com",
            'x-rapidapi-key': "8099cb7d77msh3ebb39dd41af894p154acajsnabc8c78e3d42"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)

        return json.loads(response.text)

    def launch(self):
        self.stock = pd.DataFrame(self.get_historical_data())
        self.stock['change'] = (self.stock['close'] - self.stock['open']) / self.stock['open'] * 100
        self.stock['nas_change'] = (self.nas['close'] - self.nas['open']) / self.nas['open'] * 100
        price = stat.mean(self.stock['close'])
        volume = stat.mean(self.stock['volume'])
        change = stat.mean(self.stock['change'])
        max_change = (max(self.stock['high']) - min(self.stock['low'])) / price
#         max_change = stat.mean(self.stock['max_change'])
        beta = self.stock.change.corr(self.stock.nas_change) * stat.stdev(self.stock['change']) / \
               stat.stdev(self.stock['nas_change'])
        stock_profile = self.get_profile()
        industry = stock_profile['assetProfile']['industry']
        if len(stock_profile['summaryDetail']['dividendRate']) != 0:
            dividend = stock_profile['summaryDetail']['dividendRate']['raw']
        else:
            dividend = None

        self.result.loc[-1] = [self.name, self.ticker, price, volume, change, max_change, beta, industry,
                               dividend]
        self.result = self.result.reset_index(drop=True)

        return self.result

def classificate(results):
	results.fillna(0, inplace=True)
	results['volume'] = results['volume'].astype('int')
	
	data = results.drop(['name', 'ticker'], axis=1)
	co_matrix = []
	for num in tqdm_notebook(range(len(data))):
	    each_list = list(data.loc[num])
	    each_co_matrix = []
	    for n in range(len(each_list)):
	        if n == 0:
	            if each_list[n] <= 100:
	                each_co_matrix.append('price <= 100')
	            elif 100 < each_list[n] and each_list[n] <= 500:
	                each_co_matrix.append('100 < price <= 500')
	            elif 500 < each_list[n] and each_list[n] <= 1000:
	                each_co_matrix.append('500 < price <= 1000')
	            else:
	                each_co_matrix.append('1000 < price')
	        elif n == 1:
	            if each_list[n] <= 10000:
	                each_co_matrix.append('volume <= 10000')
	            elif 10000 < each_list[n] and each_list[n] <= 100000:
	                each_co_matrix.append('10000 < volume <= 100000')
	            elif 100000 < each_list[n] and each_list[n] <= 1000000:
	                each_co_matrix.append('100000 < volume <= 1000000')
	            elif 100000 < each_list[n] and each_list[n] <= 1000000:
	                each_co_matrix.append('100000 < volume <= 1000000')
	            elif 1000000 < each_list[n] and each_list[n] <= 10000000:
	                each_co_matrix.append('1000000 < volume <= 10000000')
	            else:
	                each_co_matrix.append('10000000 < volume')
	        elif n == 2:
	            if each_list[n] <= -1:
	                each_co_matrix.append('change <= -1')
	            elif -1 < each_list[n] and each_list[n] <= -0.5:
	                each_co_matrix.append('-1 < change <= -0.5')
	            elif -0.5 < each_list[n] and each_list[n] <= 0:
	                each_co_matrix.append('-0.5 < change <= 0')
	            elif 0 < each_list[n] and each_list[n] <= 0.5:
	                each_co_matrix.append('0 < change <= 0.5')
	            elif 0.5 < each_list[n] and each_list[n] <= 1:
	                each_co_matrix.append('0.5 < change <= 1')
	            elif 1 < each_list[n]:
	                each_co_matrix.append('1 < change')
	        elif n == 3:
	            if each_list[n] <= 0.02:
	                each_co_matrix.append('max_change <= 2%')
	            elif 0.02 < each_list[n] and each_list[n] <= 0.04:
	                each_co_matrix.append('2% < max_change <= 4%')
	            elif 0.04 < each_list[n] and each_list[n] <= 0.06:
	                each_co_matrix.append('4% < max_change <= 6%')
	            elif 0.06 < each_list[n] and each_list[n] <= 0.08:
	                each_co_matrix.append('6% < max_change <= 8%')
	            elif 0.08 < each_list[n] and each_list[n] <= 0.1:
	                each_co_matrix.append('8% < max_change <=10%')
	            else:
	                each_co_matrix.append('10% < max_change')
	        elif n == 4:
	            if each_list[n] <= -1.5:
	                each_co_matrix.append('beta <= -1.5')
	            elif -1.5 < each_list[n] and each_list[n] <= -1:
	                each_co_matrix.append('-1.5 < beta <= -1')
	            elif -1 < each_list[n] and each_list[n] <= -0.5:
	                each_co_matrix.append('-1 < beta <= -0.5')
	            elif -0.5 < each_list[n] and each_list[n] <= 0:
	                each_co_matrix.append('-0.5 < beta <= -0')
	            elif 0 < each_list[n] and each_list[n] <= 0.5:
	                each_co_matrix.append('0 < beta <= 0.5')
	            elif 0.5 < each_list[n] and each_list[n] <= 1:
	                each_co_matrix.append('0.5 < beta <= 1')
	            elif 1 < each_list[n] and each_list[n] <= 1.5:
	                each_co_matrix.append('1 < beta <= 1.5')
	            elif 1.5 < each_list[n]:
	                each_co_matrix.append('1.5 < beta')
	        elif n == 5:
	            each_co_matrix.append(each_list[n])
	        elif n == 6:
	            if each_list[n] == 0:
	                each_co_matrix.append('dividendRate == 0')
	            elif 0 < each_list[n] and each_list[n] <= 2:
	                each_co_matrix.append('0 < dividendRate <= 2')
	            elif 2 < each_list[n] and each_list[n] <= 4:
	                each_co_matrix.append('2 < dividendRate <= 4')
	            elif 4 < each_list[n] and each_list[n] <= 6:
	                each_co_matrix.append('4 < dividendRate <= 6')
	            else:
	                each_co_matrix.append('6 < dividendRate')
	    co_matrix.append(each_co_matrix)
	return co_matrix

if __name__ == '__main__':
    a = '2019-07-01 21:30:00'
    timeArray = time.strptime(a, "%Y-%m-%d %H:%M:%S")
    start = str(int(time.mktime(timeArray)))

    b = '2019-09-30 21:30:00'
    timeArray = time.strptime(b, "%Y-%m-%d %H:%M:%S")
    end = str(int(time.mktime(timeArray)))

    nas = pd.DataFrame(get_nas_data(start, end))

    russo = pd.read_csv('C:/Users/wells/ctbc_project/Russo/09-30-2019.csv', encoding='utf8')
    error = russo[russo['Ticker'].isnull()].reset_index(drop=True)
    russo = russo[russo['Ticker'].notnull()].reset_index(drop=True)
    results = pd.DataFrame({})
    for i in tqdm_notebook(range(len(russo))):
        print(i)
        r = into_pool((russo.loc[i], start, end, nas))
        print(r)
        results = pd.concat([results, r], ignore_index=True)
	results.to_csv('2019-09-30 21:30:00_stock_info.csv', index=False, encoding='utf_8_sig')
	co_matrix = classificate(results)
	with open('./co_matrix.txt', 'w') as f:
		f.write(str(co_matrix))
