import unittest
import pandas as pd
import numpy as np
import torch
import sys
import os

sys.path.append(os.path.abspath('scripts'))

from Interpolation.nbeat_interpolation import generate_bidirectional_forecast

class MockModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.dummy_param = torch.nn.Parameter(torch.empty(0))
    def forward(self, x):
        # x shape: (1, 1, 2)
        # return random or zeros
        return torch.zeros(1, 1, 2) # Horizon 1, 2 features

class MockScaler:
    def transform(self, x):
        return x
    def inverse_transform(self, x):
        return x

class TestBidirectional(unittest.TestCase):
    def test_gap_fill(self):
        # Create synthetic data: 40 points
        # 0-14: Valid (15 points)
        # 15-24: Gap (10 points)
        # 25-39: Valid (15 points)
        
        data = []
        for i in range(40):
            if 15 <= i < 25:
                data.append([np.nan, np.nan])
            else:
                data.append([float(i), float(i)])
                
        df = pd.DataFrame(data, columns=['E', 'N'])
        
        model = MockModel()
        scaler = MockScaler()
        metadata = {
            'input_width': 5,
            'forecast_horizon': 1
        }
        
        # Gap indices in full DF: 15..24.
        # We need sufficient context.
        # Slice 5 to 35 (30 points).
        # Valid: 5..14 (10 points). Enough for width 5.
        # Gap: 15..24.
        # Valid: 25..34 (10 points). Enough.
        
        subset = df.iloc[5:35].copy() 
        
        result = generate_bidirectional_forecast(subset, 10, model, scaler, metadata)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 10)
        self.assertEqual(result.shape, (10, 2))
        
        print("Generated path head:", result[:2])
        
if __name__ == '__main__':
    unittest.main()
