"""
Yerevan Air Quality Project - Source Package
"""
from .data.data_loader_final import AirQualityDataLoader
from .models.baseline_model import BaselinePredictor
from .health.risk_estimation import HealthRiskEstimator
from .visualization.plots import AirQualityVisualizer

__version__ = '1.0.0'
__all__ = [
    'AirQualityDataLoader',
    'BaselinePredictor',
    'HealthRiskEstimator',
    'AirQualityVisualizer'
]