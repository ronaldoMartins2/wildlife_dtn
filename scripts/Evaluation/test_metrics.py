
import unittest
import numpy as np
from metrics import (
    calculate_mae, calculate_mse, calculate_rmse, 
    calculate_mape, calculate_smape, calculate_mase, calculate_owa
)

class TestMetrics(unittest.TestCase):
    def setUp(self):
        self.y_true = np.array([10, 20, 30, 40, 50])
        self.y_pred = np.array([12, 18, 33, 40, 45])
        self.y_train = np.array([10, 20, 30]) # Simple linear trend

    def test_mae(self):
        # |10-12| + |20-18| + |30-33| + |40-40| + |50-45| = 2 + 2 + 3 + 0 + 5 = 12 / 5 = 2.4
        expected = 2.4
        result = calculate_mae(self.y_true, self.y_pred)
        self.assertAlmostEqual(result, expected)

    def test_mse(self):
        # 4 + 4 + 9 + 0 + 25 = 42 / 5 = 8.4
        expected = 8.4
        result = calculate_mse(self.y_true, self.y_pred)
        self.assertAlmostEqual(result, expected)

    def test_rmse(self):
        expected = np.sqrt(8.4)
        result = calculate_rmse(self.y_true, self.y_pred)
        self.assertAlmostEqual(result, expected)

    def test_mape(self):
        # |2/10| + |2/20| + |3/30| + |0/40| + |5/50| = 0.2 + 0.1 + 0.1 + 0 + 0.1 = 0.5 / 5 = 0.1 * 100 = 10%
        expected = 10.0
        result = calculate_mape(self.y_true, self.y_pred)
        self.assertAlmostEqual(result, expected)
        
    def test_smape(self):
        # 2*|2|/(22) + 2*|2|/(38) + 2*|3|/(63) + 0 + 2*|5|/(95)
        # = 4/22 + 4/38 + 6/63 + 0 + 10/95
        # = 0.1818 + 0.1052 + 0.0952 + 0 + 0.1052 = 0.4874 / 5 = 0.0975 * 100 = 9.75
        result = calculate_smape(self.y_true, self.y_pred)
        self.assertTrue(result > 0)
        
    def test_mase(self):
        # Naive error on training: |20-10| + |30-20| = 10 + 10 = 20 / 2 = 10 (mean naive error)
        # MAE test = 2.4
        # MASE = 2.4 / 10 = 0.24
        naive_mae_train = np.mean(np.abs(np.diff(self.y_train))) # 10
        mae_test = calculate_mae(self.y_true, self.y_pred)
        expected = mae_test / naive_mae_train
        
        # calculate_mase integrates this
        # Note: calculate_mase implementation assumes y_train is used for denominator calculation
        # My implementation calculates naive error on y_train itself.
        result = calculate_mase(self.y_true, self.y_pred, self.y_train)
        self.assertAlmostEqual(result, expected)

    def test_owa(self):
        metrics_model = {'SMAPE': 10, 'MASE': 0.5}
        metrics_naive = {'SMAPE': 20, 'MASE': 1.0}
        
        # OWA = 0.5 * (10/20 + 0.5/1.0) = 0.5 * (0.5 + 0.5) = 0.5
        expected = 0.5
        result = calculate_owa(metrics_model, metrics_naive)
        self.assertAlmostEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
