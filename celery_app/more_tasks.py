''' Here are a few options for chp3 homework '''
from pandas_datareader import data
from celeryapp import app
import pandas as pd
from datetime import datetime, timedelta


@app.task
def current_earnings(stock):
    ''' return json response of current year ESP from yahoo finance
    params:
        stock str
    returns:
        json
    '''
    url = 'http://finance.yahoo.com/d/quotes.csv?s={}&f=se7'.format(stock)
    cy = pd.read_csv(url, names=['Stock', 'Current Year ESP'])
    return cy.to_json()


@app.task
def yoy_change(stock, source='yahoo'):
    ''' return year over year change for a given stock from today.
    params:
        stock str
    kwargs:
        source str
    returns float
    '''
    start = datetime.today() - timedelta(days=365)  # not accounting for leap yr
    df = data.DataReader(stock, source, start, datetime.today())
    return ((df.ix[-1]['Adj Close'] / df.ix[0]['Adj Close']) - 1) * 100
