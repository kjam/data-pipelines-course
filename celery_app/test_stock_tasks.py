import unittest
import json
from tasks import get_stock_info
from datetime import datetime


class TestStockInfo(unittest.TestCase):
    def setUp(self):
        self.start_date = datetime(2013, 1, 1)
        self.end_date = datetime(2013, 2, 1)
        self.stock = 'FB'

    def test_get_stock_info(self):
        result = get_stock_info(self.stock, self.start_date, self.end_date)
        self.assertIsInstance(result, str)
        result = json.loads(result)
        self.assertIn('High min', result.keys())
        stock = ' '.join(result['High min'].keys())
        self.assertEqual(stock, self.stock)
        price = result['High min'][self.stock]
        self.assertIsInstance(price, float)
        self.assertTrue(price > 0)
