import unittest
from util import *

class TestMai(unittest.TestCase):
    def setUp(self):
        self._x = [1, 2, 3, 4, 5]

    def test_std(self):
        with self.assertRaises(AssertionError):
            self.assertEqual(std(self._x, 'aa'), None)
        self.assertEqual(std(self._x, None), None)
        self.assertEqual(std(self._x, 0), None)
        self.assertEqual(std(self._x, 6), None)
        with self.assertRaises(TypeError):
            self.assertEqual(std(['a', '11'], 5), None)

    def test_ma(self):
        with self.assertRaises(AssertionError):
            self.assertEqual(ma(self._x, 'aa'), None)
        self.assertEqual(ma(self._x, None), None)
        self.assertEqual(ma(self._x, 0), None)
        self.assertEqual(ma(self._x, 6), None)

    def test_sma(self):
        with self.assertRaises(AssertionError):
            self.assertEqual(sma(self._x, 'aa', 1), None)
        self.assertEqual(sma(self._x, None, 1), None)
        self.assertEqual(sma(self._x, 0, 1), None)
        self.assertEqual(sma(self._x, 6, 1), 3)

    def test_ema(self):
        with self.assertRaises(AssertionError):
            self.assertEqual(ema(self._x, 'aa'), None)
        self.assertEqual(ema(self._x, 0), None)
        self.assertEqual(ema(self._x, -1), None)
        with self.assertRaises(AssertionError):
            self.assertEqual(ema('a', 5), None)
        with self.assertRaises(TypeError):
            self.assertEqual(ema([-1.2, 2.5, 3.21], 5), None)

if __name__ == '__main__':
    unittest.main()
