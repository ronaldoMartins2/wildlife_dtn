import numpy as np

def calculate_mae(y_true, y_pred):
    """Mean Absolute Error"""
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean(np.abs(y_true - y_pred))

def calculate_mse(y_true, y_pred):
    """Mean Squared Error"""
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean((y_true - y_pred)**2)

def calculate_rmse(y_true, y_pred):
    """Root Mean Squared Error"""
    return np.sqrt(calculate_mse(y_true, y_pred))

def calculate_mape(y_true, y_pred):
    """Mean Absolute Percentage Error"""
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    # Avoid division by zero
    mask = y_true != 0
    if not mask.any():
        return np.inf
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

def calculate_smape(y_true, y_pred):
    """Symmetric Mean Absolute Percentage Error"""
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    denominator = (np.abs(y_true) + np.abs(y_pred))
    # Avoid division by zero
    mask = denominator != 0
    if not mask.any():
        return 0.0
    return 100 * np.mean(2 * np.abs(y_pred[mask] - y_true[mask]) / denominator[mask])

def calculate_mase(y_true, y_pred, y_train, seasonality=1):
    """
    Mean Absolute Scaled Error
    
    y_train: Training data used to calculate the naive forecast error (denominator).
    seasonality: Seasonality of the data for naive forecast (default 1 for non-seasonal/random walk).
    """
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    y_train = np.array(y_train)
    
    mae = calculate_mae(y_true, y_pred)
    
    # Calculate Mean Absolute Error of the Naive Forecast on the training set
    # Naive forecast: y_hat[t] = y[t-seasonality]
    n = len(y_train)
    if n <= seasonality:
        return np.nan # Cannot calculate naive error
        
    naive_errors = np.abs(y_train[seasonality:] - y_train[:-seasonality])
    mae_naive = np.mean(naive_errors)
    
    if mae_naive == 0:
        return np.inf # Avoid division by zero if training data is constant
        
    return mae / mae_naive

def calculate_owa(metrics_model, metrics_naive):
    """
    Overall Weighted Average
    
    OWA = 0.5 * (SMApe_model / SMAPE_naive) + 0.5 * (MASE_model / MASE_naive)
    
    Note: MASE_naive should theoretically be 1 (if calculated on the same data/horizon), 
    but here we accept the computed value.
    If comparing against Naive forecast itself, metrics_naive refers to the Naive forecast's performance on the TEST set.
    """
    
    smape_model = metrics_model.get('SMAPE', 0)
    smape_naive = metrics_naive.get('SMAPE', 1) 
    
    mase_model = metrics_model.get('MASE', 0)
    mase_naive = metrics_naive.get('MASE', 1)
    
    if smape_naive == 0: smape_ratio = 1.0 # Should not happen if data varies
    else: smape_ratio = smape_model / smape_naive
        
    if mase_naive == 0: mase_ratio = 1.0
    else: mase_ratio = mase_model / mase_naive
    
    return 0.5 * (smape_ratio + mase_ratio)
