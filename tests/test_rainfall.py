# import sys
# sys.path.insert(0, '../src')
import unittest
import numpy as np
from utils import rainfall

class TestRainfallFunctions(unittest.TestCase):
    def test_level_to_ordinal_encoding(self):
        self.assertTrue(np.array_equal(rainfall.level_to_ordinal_encoding(0), np.array([1, 0, 0, 0, 0])))
        self.assertTrue(np.array_equal(rainfall.level_to_ordinal_encoding(1), np.array([1, 1, 0, 0, 0])))
        self.assertTrue(np.array_equal(rainfall.level_to_ordinal_encoding(2), np.array([1, 1, 1, 0, 0])))
        self.assertTrue(np.array_equal(rainfall.level_to_ordinal_encoding(3), np.array([1, 1, 1, 1, 0])))
        self.assertTrue(np.array_equal(rainfall.level_to_ordinal_encoding(4), np.array([1, 1, 1, 1, 1])))

    def test_binary_encoding_to_level(self):
        one_hot_array = np.array([[1, 0], [0, 1], [0, 1]])
        expected_levels = [0, 1, 1]
        self.assertEqual(rainfall.binary_encoding_to_level(one_hot_array), expected_levels)

    def test_ordinal_encoding_to_level(self):
        pred = np.array([[0.9, 0.1, 0.1, 0.1], [0.9, 0.9, 0.1, 0.1], [0.9, 0.9, 0.9, 0.1]])
        expected_levels = np.array([0, 1, 2])
        self.assertTrue(np.array_equal(rainfall.ordinal_encoding_to_level(pred), expected_levels))

    def test_value_to_level(self):
        y_value = np.array([0., 0.2, 0.4, 1., 1.2, 20.2, 42.2, 54.8, 60.8])
        expected_levels = np.array([0, 1, 1, 1, 1, 2, 3, 4, 4])
        self.assertTrue(np.array_equal(rainfall.value_to_level(y_value), expected_levels))

    def test_value_to_ordinal_encoding(self):
        y_values = np.array([0., 0.2, 0.4, 1., 1.2, 20.2, 42.2, 54.8, 60.8])
        expected_encoding = np.array([
            [1, 0, 0, 0, 0],

            [1, 1, 0, 0, 0],
            [1, 1, 0, 0, 0],
            [1, 1, 0, 0, 0],
            [1, 1, 0, 0, 0],

            [1, 1, 1, 0, 0],

            [1, 1, 1, 1, 0],

            [1, 1, 1, 1, 1],
            [1, 1, 1, 1, 1]
        ])

        # print("*-*")
        # print(value_to_ordinal_encoding(y_values))
        # print(expected_encoding)
        # print("*-*")

        self.assertTrue(np.array_equal(rainfall.value_to_ordinal_encoding(y_values), expected_encoding))


if __name__ == '__main__':
    unittest.main()
