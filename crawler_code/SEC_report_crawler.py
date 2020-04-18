import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm_notebook
import time

class Sec_New_Report_Crawler:
    def __init__(self, url):
        self.urls = [url]
        self.documents = []
        self.html = []
        self.html_time = []
        self.txt = []
        self.txt_time = []
    def get_all_url(self):
        for url in tqdm_notebook(self.urls):
            res = requests.get(url)
            soup = BeautifulSoup(res.text,'html.parser')
            clean = soup.select('input')
            if clean[-1].get('value') == 'Next 100':
                self.urls.append('https://www.sec.gov' + clean[-1].get('onclick').split("\'")[1])
            clean = soup.select('a')
            for item in clean:
                if item.get('id') == 'documentsbutton':
                    self.documents.append('https://www.sec.gov' + item.get('href'))
    def get_all_document(self):
        for url in tqdm_notebook(self.documents):
            res = requests.get(url)
            soup = BeautifulSoup(res.text,'html.parser')
            clean = soup.select('a')
            clean_time = soup.find_all("div", class_="info")
            for item in clean:
                try:
                    if 'htm' in item.string:
                        self.html.append('https://www.sec.gov' + item.get('href'))
                        self.html_time.append(clean_time[3].string)
                    elif 'txt' in item.string:
                        self.txt.append('https://www.sec.gov' + item.get('href'))
                        self.txt_time.append(clean_time[3].string)
                except:
                    None
    def load_newtype_report(self):
        for num in tqdm_notebook(range(len(self.html))):
            try:
                df = pd.read_html(self.html[num])[3][3:].reset_index(drop = True)
                df.columns = ['NAME OF ISSUER', 'TITLE OF CLASS', 'CUSIP', 'VALUE', 'SHRS OR PRN AMT',
                   'SH/PRN', 'PUT/CALL', 'INVESTMENT DISCRETION', 'OTHER MANAGER', 'SOLE',
                   'SHARED', 'NONE']
                df.to_csv('Russo/' + self.html_time[num] + '_' + str(num) + '.csv', index = False)
            except:
                continue

if __name__ == '__main__':
    snrc = Sec_New_Report_Crawler('https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000860643&type=&dateb=&owner=exclude&start=0&count=100')
    snrc.get_all_url()
    snrc.get_all_document()
    snrc.load_newtype_report()
