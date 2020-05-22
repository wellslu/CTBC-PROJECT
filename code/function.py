import pandas as pd
class test_function:
    def last_price(each):
        ticker, df = each
        print(ticker)
        df.loc[:, 'Last_PRC'] = df['PRC'].shift()
        return df