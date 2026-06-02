"""
Biological and Ecological Quality Metrics for Wildlife Trajectory Interpolation

This module provides metrics that evaluate interpolated trajectories from a biologically
meaningful perspective, complementing geometric metrics like ADE and FDE.

Metrics included:
1. Infeasible Steps Ratio: Percentage of interpolated steps with biologically implausible velocities
2. Turning Angles Distribution: KL divergence between predicted and real turning angle distributions
3. Dynamic Time Warping (DTW): Spatial trajectory similarity ignoring temporal misalignments
"""

import numpy as np
from scipy.stats import entropy
from scipy.stats import gaussian_kde
try:
    from dtaidistance import dtw
except ImportError:
    dtw = None


def haversine_distance(lon1, lat1, lon2, lat2):
    """
    Calculate great-circle distance in meters between two lat/lon points.
    
    Parameters:
    -----------
    lon1, lat1, lon2, lat2 : float or array-like
        Coordinates in decimal degrees
        
    Returns:
    --------
    float or np.ndarray
        Distance in meters
    """
    from math import radians, sin, cos, atan2, sqrt
    
    lon1, lat1, lon2, lat2 = np.radians([lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    
    R = 6371000.0  # Earth's radius in meters
    return R * c


def calculate_infeasible_steps_ratio(lons_true, lats_true, times_true, 
                                      lons_pred, lats_pred, times_pred,
                                      species='jaguar', return_details=False):
    """
    Calculate the percentage of interpolated steps with biologically implausible velocities.
    
    Parameters:
    -----------
    lons_true, lats_true : array-like
        Ground truth coordinates (degrees)
    times_true : array-like
        Ground truth timestamps (can be in any unit, as long as consistent)
    lons_pred, lats_pred : array-like
        Predicted/interpolated coordinates (degrees)
    times_pred : array-like
        Predicted/interpolated timestamps (same unit as times_true)
    species : str, default='jaguar'
        Species being tracked. Used to determine speed limits:
        - 'jaguar': max 25 m/s (90 km/h) - confirmed field data
        - 'puma': max 20 m/s (72 km/h)
        - 'ocelot': max 15 m/s (54 km/h)
        - 'bird': max 20 m/s (72 km/h)
    return_details : bool, default=False
        If True, also return array of step speeds
        
    Returns:
    --------
    ratio : float
        Percentage of infeasible steps (0-1)
    details : dict (optional)
        Contains 'speeds' (m/timestep), 'threshold', 'infeasible_indices'
    """
    # Species-specific maximum speeds (m/s converted to m per timestep)
    speed_limits = {
        'jaguar': 25.0,      # 90 km/h
        'puma': 20.0,        # 72 km/h
        'ocelot': 15.0,      # 54 km/h
        'bird': 20.0,        # 72 km/h
    }
    
    max_speed = speed_limits.get(species.lower(), 25.0)
    
    # Calculate distances and times for predicted trajectory
    lons_pred = np.array(lons_pred)
    lats_pred = np.array(lats_pred)
    times_pred = np.array(times_pred, dtype=float)
    
    if len(lons_pred) < 2:
        return 0.0, None if return_details else 0.0
    
    # Calculate step distances (meters)
    distances = np.array([
        haversine_distance(lons_pred[i], lats_pred[i], 
                          lons_pred[i+1], lats_pred[i+1])
        for i in range(len(lons_pred) - 1)
    ])
    
    # Calculate time differences
    time_diffs = np.diff(times_pred)
    
    # Avoid division by zero
    valid_mask = time_diffs > 0
    if not valid_mask.any():
        return 0.0, None if return_details else 0.0
    
    # Calculate speeds (m per timestep)
    speeds = np.zeros(len(time_diffs))
    speeds[valid_mask] = distances[valid_mask] / time_diffs[valid_mask]
    
    # Identify infeasible steps (speed exceeds limit)
    infeasible_mask = speeds > max_speed
    infeasible_ratio = float(np.sum(infeasible_mask)) / len(speeds)
    
    if return_details:
        return infeasible_ratio, {
            'speeds': speeds,
            'threshold': max_speed,
            'infeasible_indices': np.where(infeasible_mask)[0],
            'max_speed_observed': float(np.max(speeds)),
            'mean_speed': float(np.mean(speeds[valid_mask])),
        }
    
    return infeasible_ratio


def calculate_turning_angles_kl_divergence(lons_true, lats_true,
                                           lons_pred, lats_pred,
                                           n_bins=36, return_details=False):
    """
    Calculate KL divergence between turning angle distributions of real vs predicted trajectories.
    
    Turning angles measure the direction changes at each point (formed by 3 consecutive points).
    This metric captures whether the interpolated trajectory has similar sinuosity to the real one.
    
    Parameters:
    -----------
    lons_true, lats_true : array-like
        Ground truth coordinates (degrees)
    lons_pred, lats_pred : array-like
        Predicted/interpolated coordinates (degrees)
    n_bins : int, default=36
        Number of bins for angle histogram (10-degree bins)
    return_details : bool, default=False
        If True, also return angle arrays and histograms
        
    Returns:
    --------
    kl_div : float
        KL divergence (0 = identical distributions, higher = more different)
    details : dict (optional)
        Contains 'angles_true', 'angles_pred', 'hist_true', 'hist_pred'
    """
    def calculate_angles(lons, lats):
        """Calculate turning angles for a trajectory."""
        lons = np.array(lons)
        lats = np.array(lats)
        
        if len(lons) < 3:
            return np.array([])
        
        # Calculate vectors between consecutive points
        vectors = np.array([
            [lons[i+1] - lons[i], lats[i+1] - lats[i]]
            for i in range(len(lons) - 1)
        ])
        
        # Calculate angles between consecutive vectors
        angles = []
        for i in range(len(vectors) - 1):
            v1 = vectors[i]
            v2 = vectors[i + 1]
            
            # Use atan2 for better numerical stability
            angle1 = np.arctan2(v1[1], v1[0])
            angle2 = np.arctan2(v2[1], v2[0])
            
            # Calculate turning angle (difference in bearings)
            turn = np.degrees(angle2 - angle1)
            # Normalize to [-180, 180]
            turn = ((turn + 180) % 360) - 180
            angles.append(turn)
        
        return np.array(angles)
    
    angles_true = calculate_angles(lons_true, lats_true)
    angles_pred = calculate_angles(lons_pred, lats_pred)
    
    if len(angles_true) == 0 or len(angles_pred) == 0:
        return 0.0, None if return_details else 0.0
    
    # Create histograms
    bins = np.linspace(-180, 180, n_bins + 1)
    hist_true, _ = np.histogram(angles_true, bins=bins)
    hist_pred, _ = np.histogram(angles_pred, bins=bins)
    
    # Normalize to probability distributions
    hist_true = hist_true / np.sum(hist_true)
    hist_pred = hist_pred / np.sum(hist_pred)
    
    # Add small epsilon to avoid log(0)
    epsilon = 1e-10
    hist_true = hist_true + epsilon
    hist_pred = hist_pred + epsilon
    hist_true = hist_true / np.sum(hist_true)
    hist_pred = hist_pred / np.sum(hist_pred)
    
    # Calculate KL divergence: KL(true || pred)
    kl_div = float(entropy(hist_true, hist_pred))
    
    if return_details:
        return kl_div, {
            'angles_true': angles_true,
            'angles_pred': angles_pred,
            'hist_true': hist_true,
            'hist_pred': hist_pred,
            'mean_angle_true': float(np.mean(angles_true)),
            'mean_angle_pred': float(np.mean(angles_pred)),
            'std_angle_true': float(np.std(angles_true)),
            'std_angle_pred': float(np.std(angles_pred)),
        }
    
    return kl_div


def calculate_dtw_distance(lons_true, lats_true,
                           lons_pred, lats_pred,
                           normalize=True, return_details=False):
    """
    Calculate Dynamic Time Warping distance between trajectories.
    
    DTW measures spatial trajectory similarity while being robust to small temporal
    misalignments. This is useful when the interpolated trajectory has the right shape
    but points are slightly shifted in time.
    
    Parameters:
    -----------
    lons_true, lats_true : array-like
        Ground truth coordinates (degrees)
    lons_pred, lats_pred : array-like
        Predicted/interpolated coordinates (degrees)
    normalize : bool, default=True
        If True, normalize by max(len(true), len(pred)) to make it comparable
    return_details : bool, default=False
        If True, also return DTW cost matrix
        
    Returns:
    --------
    dtw_distance : float
        Normalized DTW distance in meters (0 = identical, higher = different)
    details : dict (optional)
        Contains 'raw_distance', 'cost_matrix'
    """
    if dtw is None:
        raise ImportError(
            "dtaidistance package required for DTW calculation. "
            "Install with: pip install dtaidistance"
        )
    
    lons_true = np.array(lons_true)
    lats_true = np.array(lats_true)
    lons_pred = np.array(lons_pred)
    lats_pred = np.array(lats_pred)
    
    if len(lons_true) < 2 or len(lons_pred) < 2:
        return 0.0, None if return_details else 0.0
    
    # Create 2D coordinate arrays
    coords_true = np.column_stack([lons_true, lats_true])
    coords_pred = np.column_stack([lons_pred, lats_pred])
    
    # Define pairwise distance function (Haversine in meters)
    def pairwise_haversine(c1, c2):
        return haversine_distance(c1[0], c1[1], c2[0], c2[1])
    
    # Calculate DTW distance
    # Create distance matrix
    dist_matrix = np.zeros((len(coords_true), len(coords_pred)))
    for i in range(len(coords_true)):
        for j in range(len(coords_pred)):
            dist_matrix[i, j] = pairwise_haversine(coords_true[i], coords_pred[j])
    
    # Calculate DTW using dtaidistance
    raw_dtw = dtw.distance(dist_matrix)
    
    # Normalize by the number of steps
    if normalize:
        norm_factor = max(len(coords_true), len(coords_pred))
        normalized_dtw = raw_dtw / norm_factor
    else:
        normalized_dtw = raw_dtw
    
    if return_details:
        return float(normalized_dtw), {
            'raw_distance': float(raw_dtw),
            'norm_factor': max(len(coords_true), len(coords_pred)),
            'distance_matrix': dist_matrix,
        }
    
    return float(normalized_dtw)


def calculate_sinuosity(lons, lats):
    """
    Calculate sinuosity (tortuosity) of a trajectory.
    
    Sinuosity = path length / straight-line distance
    Values close to 1 = straight path, higher values = more tortuous
    
    Parameters:
    -----------
    lons, lats : array-like
        Coordinates in degrees
        
    Returns:
    --------
    sinuosity : float
        Sinuosity index
    """
    lons = np.array(lons)
    lats = np.array(lats)
    
    if len(lons) < 2:
        return 1.0
    
    # Calculate path length (sum of all step distances)
    distances = np.array([
        haversine_distance(lons[i], lats[i], lons[i+1], lats[i+1])
        for i in range(len(lons) - 1)
    ])
    path_length = np.sum(distances)
    
    # Calculate straight-line distance
    straight_line = haversine_distance(lons[0], lats[0], lons[-1], lats[-1])
    
    if straight_line == 0:
        return 1.0
    
    sinuosity = path_length / straight_line
    return float(sinuosity)


def calculate_area_difference(lons_true, lats_true,
                              lons_pred, lats_pred):
    """
    Calculate difference in area swept by the trajectory polygon.
    
    Uses the Shoelace formula to compute polygon area, which captures
    the overall "spread" or area covered by the trajectory.
    
    Parameters:
    -----------
    lons_true, lats_true : array-like
        Ground truth coordinates (degrees)
    lons_pred, lats_pred : array-like
        Predicted/interpolated coordinates (degrees)
        
    Returns:
    --------
    area_diff_ratio : float
        (Area_pred - Area_true) / Area_true
        Positive = predicted covers larger area, Negative = smaller area
    """
    def polygon_area(lons, lats):
        """Calculate area using Shoelace formula (2*area)."""
        lons = np.array(lons)
        lats = np.array(lats)
        
        if len(lons) < 3:
            return 0.0
        
        # Use Shoelace formula
        area = 0.5 * np.abs(
            np.dot(lons[:-1], lats[1:]) - np.dot(lons[1:], lats[:-1])
        )
        return float(area)
    
    area_true = polygon_area(lons_true, lats_true)
    area_pred = polygon_area(lons_pred, lats_pred)
    
    if area_true == 0:
        return 0.0
    
    return (area_pred - area_true) / area_true


def calculate_frechet_distance(lons_true, lats_true,
                               lons_pred, lats_pred):
    """
    Calculate Fréchet distance between two trajectories.
    
    Fréchet distance measures the minimum distance a point must travel
    if it follows the curves, useful for comparing trajectory shapes.
    
    Parameters:
    -----------
    lons_true, lats_true : array-like
        Ground truth coordinates (degrees)
    lons_pred, lats_pred : array-like
        Predicted/interpolated coordinates (degrees)
        
    Returns:
    --------
    frechet_distance : float
        Fréchet distance in meters
    """
    lons_true = np.array(lons_true)
    lats_true = np.array(lats_true)
    lons_pred = np.array(lons_pred)
    lats_pred = np.array(lats_pred)
    
    if len(lons_true) < 2 or len(lons_pred) < 2:
        return 0.0
    
    # Compute pairwise distances
    distances = np.zeros((len(lons_true), len(lons_pred)))
    for i in range(len(lons_true)):
        for j in range(len(lons_pred)):
            distances[i, j] = haversine_distance(
                lons_true[i], lats_true[i],
                lons_pred[j], lats_pred[j]
            )
    
    # Compute forward and backward distances
    forward = np.zeros_like(distances)
    backward = np.zeros_like(distances)
    
    forward[0, 0] = distances[0, 0]
    for i in range(1, len(lons_true)):
        forward[i, 0] = max(forward[i-1, 0], distances[i, 0])
    for j in range(1, len(lons_pred)):
        forward[0, j] = max(forward[0, j-1], distances[0, j])
    
    for i in range(1, len(lons_true)):
        for j in range(1, len(lons_pred)):
            forward[i, j] = max(
                min(forward[i-1, j], forward[i, j-1], forward[i-1, j-1]),
                distances[i, j]
            )
    
    return float(forward[-1, -1])


def compute_all_biological_metrics(lons_true, lats_true, times_true,
                                    lons_pred, lats_pred, times_pred,
                                    species='jaguar'):
    """
    Compute all biological/ecological metrics in one go.
    
    Parameters:
    -----------
    lons_true, lats_true : array-like
        Ground truth coordinates
    times_true : array-like
        Ground truth timestamps
    lons_pred, lats_pred : array-like
        Predicted coordinates
    times_pred : array-like
        Predicted timestamps
    species : str
        Species identifier for speed limits
        
    Returns:
    --------
    metrics : dict
        Dictionary with all computed metrics and sub-metrics
    """
    metrics = {}
    
    # Infeasible steps
    infeasible_ratio, infeasible_details = calculate_infeasible_steps_ratio(
        lons_true, lats_true, times_true,
        lons_pred, lats_pred, times_pred,
        species=species, return_details=True
    )
    metrics['infeasible_steps_ratio'] = infeasible_ratio
    metrics['infeasible_steps_details'] = infeasible_details
    
    # Turning angles
    kl_div, angles_details = calculate_turning_angles_kl_divergence(
        lons_true, lats_true,
        lons_pred, lats_pred,
        return_details=True
    )
    metrics['turning_angles_kl_divergence'] = kl_div
    metrics['turning_angles_details'] = angles_details
    
    # DTW distance
    try:
        dtw_dist = calculate_dtw_distance(
            lons_true, lats_true,
            lons_pred, lats_pred,
            normalize=True
        )
        metrics['dtw_distance_normalized'] = dtw_dist
    except ImportError:
        metrics['dtw_distance_normalized'] = None
    
    # Sinuosity
    sinuosity_true = calculate_sinuosity(lons_true, lats_true)
    sinuosity_pred = calculate_sinuosity(lons_pred, lats_pred)
    metrics['sinuosity_true'] = sinuosity_true
    metrics['sinuosity_pred'] = sinuosity_pred
    metrics['sinuosity_ratio'] = sinuosity_pred / sinuosity_true if sinuosity_true > 0 else 1.0
    
    # Area
    area_diff = calculate_area_difference(lons_true, lats_true, lons_pred, lats_pred)
    metrics['area_difference_ratio'] = area_diff
    
    # Fréchet distance
    frechet_dist = calculate_frechet_distance(lons_true, lats_true, lons_pred, lats_pred)
    metrics['frechet_distance_meters'] = frechet_dist
    
    return metrics


if __name__ == "__main__":
    # Example usage
    print("Biological Metrics Module - For use in interpolation evaluation scripts")
    
    # Test data
    lons_true = np.array([-60.5, -60.501, -60.502, -60.503, -60.504])
    lats_true = np.array([-3.0, -3.001, -3.002, -3.003, -3.004])
    times_true = np.array([0, 1, 2, 3, 4])
    
    lons_pred = np.array([-60.5, -60.5005, -60.501, -60.5015, -60.502, -60.5025, -60.503, -60.5035, -60.504])
    lats_pred = np.array([-3.0, -3.0005, -3.001, -3.0015, -3.002, -3.0025, -3.003, -3.0035, -3.004])
    times_pred = np.array([0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4])
    
    metrics = compute_all_biological_metrics(
        lons_true, lats_true, times_true,
        lons_pred, lats_pred, times_pred,
        species='jaguar'
    )
    
    print("\nComputed Metrics:")
    for key, value in metrics.items():
        if not isinstance(value, dict):
            print(f"  {key}: {value}")
