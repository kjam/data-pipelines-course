import json
from tasks import get_stock_info
from datetime import datetime


def test_get_stock_info():
    start_date = datetime(2013, 1, 1)
    end_date = datetime(2013, 2, 1)
    stock = 'FB'

    result = get_stock_info(stock, start_date, end_date)
    assert isinstance(result, str)
    result = json.loads(result)
    assert 'High min' in result.keys()
    stock = ' '.join(result['High min'].keys())
    assert stock == stock
    price = result['High min'][stock]
    assert isinstance(price, float)
    assert price > 0
