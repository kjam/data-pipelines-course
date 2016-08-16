''' Task module for showing celery functionality. '''
from pandas_datareader import data
import pandas as pd


def get_stock_info(stock, start, end, source='yahoo'):
    ''' Collect aggregate info for a stock given a daterange.
        params:
            stock: str
            start: datetime
            end: datetime
        kwargs:
            source (optional): str
        returns:
            json
    '''
    df = data.DataReader(stock, source, start, end)
    df['Stock'] = stock
    agg = df.groupby('Stock').agg({
        'Open': ['min', 'max', 'mean', 'median'],
        'Adj Close': ['min', 'max', 'mean', 'median'],
        'Close': ['min', 'max', 'mean', 'median'],
        'High': ['min', 'max', 'mean', 'median'],
        'Low': ['min', 'max', 'mean', 'median'],
    })
    agg.columns = [' '.join(col).strip() for col in agg.columns.values]
    return agg.to_json()


def calc_ratio(price, compare):
    ''' Calculates percentage ratio when given stock price and comparison price
    params:
        price: float
        compare: float
    returns float
    '''
    return round(((price / compare) - 1) * 100, 2)


def price_range(stock, start, end, source='yahoo'):
    ''' Compare today's date to see if it is near max or min of closing prices
        in certain daterange.
    params:
        stock: str
        start: datetime
        end: datetime
    kwargs:
        source (optional): str
    returns:
        string
    '''
    df = data.DataReader(stock, source, start, end)
    period_high = df['Adj Close'].max()
    period_low = df['Adj Close'].min()
    url = 'http://finance.yahoo.com/d/quotes.csv?s={}&f=sat1'.format(stock)
    td = pd.read_csv(url, names=['Stock', 'Price', 'Last Trade'])
    td_price = td['Price'].mean()
    if abs(td_price - period_high) < abs(td_price - period_low):
        return 'Price is higher: {}% change from period high ({})'.format(
            calc_ratio(td_price, period_high), period_high)
    return 'Price is lower: {}% change from period low ({})'.format(
        calc_ratio(td_price, period_low), period_low)
