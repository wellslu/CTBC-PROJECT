import pandas as pd
import requests
from bs4 import BeautifulSoup


def crawler(url):
    col = ['Name', 'Ticker', 'Class', 'Shares', 'Change', 'Value', 'Change', '% Port', '% OS', 'Hist', 'Date']
    results = pd.DataFrame({})
    for option in tqdm_notebook(options):
        res = requests.get(url)
        soup = BeautifulSoup(res.text,'html.parser')
        options = [soup.select('select')[0].select('option')[i].get('value') for i in range(len(soup.select('select')[0].select('option')))]
        options = options[1:]
        __EVENTVALIDATION = soup.select("input")[3].get('value')
        __VIEWSTATE = soup.select("input")[1].get('value')


        formdata = {'ctrlHeader_toolkitScriptManager_HiddenField': ';;AjaxControlToolkit, Version=4.1.40412.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:acfc7575-cdee-46af-964f-5d85d9cdcf92:475a4ef5:5546a2b:d2e10b12:effe2a26:37e2e5c9:5a682656:12bbc599:7e63a579',
        '__EVENTTARGET': 'lstPeriods',
        '__VIEWSTATE': __VIEWSTATE,
        '__VIEWSTATEGENERATOR': 'B3D39D0A',
        '__EVENTVALIDATION': __EVENTVALIDATION,
        'lstPeriods': option,
        'hiddenInputToUpdateATBuffer_CommonToolkitScripts': '1'}
        res = requests.post(url, data = formdata)
        soup = BeautifulSoup(res.text,'html.parser')
        __EVENTVALIDATION = soup.select("input")[3].get('value')
        __VIEWSTATE = soup.select("input")[1].get('value')
        df = pd.read_html(str(soup))[3][1:-1]
        df.columns = col
        results = pd.concat([results, df], ignore_index=True)
        if len(df) != 0:
            while True:
                formdata = {'ctrlHeader_toolkitScriptManager_HiddenField': ';;AjaxControlToolkit, Version=4.1.40412.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:acfc7575-cdee-46af-964f-5d85d9cdcf92:475a4ef5:5546a2b:d2e10b12:effe2a26:37e2e5c9:5a682656:12bbc599:7e63a579',
                '__EVENTTARGET': 'gridPortfolio$ctl39$ctl01',
                '__VIEWSTATE': __VIEWSTATE,
                '__VIEWSTATEGENERATOR': 'B3D39D0A',
                '__EVENTVALIDATION': __EVENTVALIDATION,
                'lstPeriods': option,
                'hiddenInputToUpdateATBuffer_CommonToolkitScripts': '1'}
                res = requests.post(url, data = formdata)
                soup = BeautifulSoup(res.text,'html.parser')
                __EVENTVALIDATION = soup.select("input")[3].get('value')
                __VIEWSTATE = soup.select("input")[1].get('value')
                df = pd.read_html(str(soup))[3][1:-1]
                df.columns = col
                if list(df[-2:-1]['Ticker']) == list(results[-2:-1]['Ticker']) and list(df[-2:-1]['Date']) == list(results[-2:-1]['Date']):
                    break
                results = pd.concat([results, df], ignore_index=True)
    return results
