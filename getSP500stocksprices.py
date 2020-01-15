import datetime
import sqlite3
import pandas as pd
import pandas_datareader.data as web
import requests
import bs4 as bs

def get_SP500tickers():
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        ticker = ticker.strip('\n')
        tickers.append(ticker)
    return tickers

def read_datafromweb(assets, startDate, endDate, source = 'yahoo'):
    prices = {}
    volumes = {}
    for asset in assets:
        try:
            df = web.DataReader(asset, source, start=startDate, end=endDate)
            prices[asset] = df['Adj Close']
            volumes[asset] = df['Volume']
        except:
            print('Error: Skipping', asset)

    prices = pd.DataFrame(prices)
    volumes = pd.DataFrame(volumes)
    data = {'Price':prices,'Volume': volumes}
    return data

def main():
    start = datetime.date(2016,12,20)
    end = datetime.date.today()
    AssetList = get_SP500tickers()
    Data = read_datafromweb(AssetList, start, end)


    conn = sqlite3.connect('Data.db')
    Data['Price'].to_sql('Price_db', conn, if_exists='append')

    # Try to retrieve the data back
    price_back = pd.read_sql('select * from Price_db', conn)
    print(price_back.head())

if __name__ == '__main__':
    main()