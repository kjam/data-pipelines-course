''' Task module for showing celery functionality. '''
from pandas_datareader import data
from celeryapp import app
from urllib.error import HTTPError, URLError
import pandas as pd
import logging


@app.task
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
    logging.debug('start and end types are: %s %s', type(start), type(end))
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


@app.task(bind=True)
def price_range(self, stock, start, end, source='yahoo'):
    ''' Compare today's date to see if it is near max or min of closing prices
        in certain daterange.
    params:
        stock: str
        start: datetime
        end: datetime
    kwargs:
        source (optional): str
    returns:
        dictionary
    '''
    df = data.DataReader(stock, source, start, end)
    period_high = df['Adj Close'].max()
    period_mean = df['Adj Close'].mean()
    period_low = df['Adj Close'].min()
    resp = {
        'stock': stock,
        'period_high': period_high,
        'period_low': period_low,
        'period_mean': period_mean,
        'period_start': start,
        'period_end': end,
    }
    url = 'http://finance.yahoo.com/d/quotes.csv?s={}&f=sat1'.format(stock)
    try:
        td = pd.read_csv(url, names=['Stock', 'Price', 'Last Trade'])
    except (HTTPError, URLError) as exc:
        logging.exception('pandas read_csv error for yahoo finance URL: %s',
                          url)
        raise self.retry(exc=exc)
    td_price = td['Price'].mean()
    resp['todays_price'] = td_price
    if abs(td_price - period_high) < abs(td_price - period_low):
        resp['result'] = 'higher'
    else:
        resp['result'] = 'lower'
    resp['percent_change'] = calc_ratio(td_price, period_mean)
    return resp


@app.task
def determine_buy(result):
    ''' Extremely naive buy logic (for example's sake)
    params:
        result: json result from price_range task
    return:
        boolean
    '''
    if result['result'] == 'lower':
        return True
    return False


@app.task
def sort_results(results, key='todays_price'):
    ''' Sort by given key, defaults to todays_price
    params:
        results: list of results from price_range task
    kwargs:
        key: str (must be in price_range return dictionary)
    return sorted list
    '''
    return sorted(results, key=lambda x: x[key])
