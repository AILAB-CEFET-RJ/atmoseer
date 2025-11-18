import numpy as np
from enum import Enum
from sklearn.preprocessing import OneHotEncoder
import math

binary_classification_thresholds_dict = {
    "NO_RAIN":      (0.0, 0.0), 
    "RAIN":         (0.4, math.inf)
}

# see http://alertario.rio.rj.gov.br/previsao-do-tempo/termosmet/
multiclass_classification_thresholds_dict = {
    "NO_RAIN":          (0.0, 0.0), 
    "WEAK_RAIN":        (0.0, 5.0),
    "MODERATE_RAIN":    (5.0, 25.0),
    "STRONG_RAIN":      (25.0, 50.0),
    "EXTREME_RAIN":     (50.0, math.inf)
}

class ExtendedEnum(Enum):
    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))

class ForecastingTask(ExtendedEnum):
    REGRESSION = 'REGRESSION'
    ORDINAL_CLASSIFICATION = 'ORDINAL_CLASSIFICATION'
    BINARY_CLASSIFICATION = 'BINARY_CLASSIFICATION'

class BinaryPrecipitationLevel(Enum):
    NO_RAIN = 0
    RAIN = 1

class OrdinalPrecipitationLevel(Enum):
    NONE = 0
    WEAK = 1
    MODERATE = 2
    STRONG = 3
    EXTREME = 4


def level_to_ordinal_encoding(y_level):
    """
    Convert levels to ordinal encodings, e.g.
        0 --> [0.9, 0.1, 0.1, 0.1]
        1 --> [0.9, 0.9, 0.1, 0.1]
        2 --> [0.9, 0.9, 0.9, 0.1]
    """
    if y_level == OrdinalPrecipitationLevel.NONE.value:
        return np.array([1, 0, 0, 0, 0])
    elif y_level == OrdinalPrecipitationLevel.WEAK.value:
        return np.array([1, 1, 0, 0, 0])
    elif y_level == OrdinalPrecipitationLevel.MODERATE.value:
        return np.array([1, 1, 1, 0, 0])
    elif y_level == OrdinalPrecipitationLevel.STRONG.value:
        return np.array([1, 1, 1, 1, 0])
    elif y_level == OrdinalPrecipitationLevel.EXTREME.value:
        return np.array([1, 1, 1, 1, 1])

def value_to_ordinal_encoding(y_values):
    y_levels = value_to_level(y_values)
    y_encoded = np.array(list(map(level_to_ordinal_encoding, y_levels)))
    return y_encoded

def ordinal_encoding_to_level(y_encoded: np.ndarray):
    """
    Convert ordinal predictions to class labels, e.g.
        [0.9, 0.1, 0.1, 0.1] -> 0
        [0.9, 0.9, 0.1, 0.1] -> 1
        [0.9, 0.9, 0.9, 0.1] -> 2
    """
    return (y_encoded > 0.5).cumprod(axis=1).sum(axis=1) - 1

def value_to_level(y_values):
    none_idx, weak_idx, moderate_idx, strong_idx, extreme_idx = get_events_per_level(y_values)
    y_ordinal_levels = np.zeros_like(y_values)
    y_ordinal_levels[none_idx] = OrdinalPrecipitationLevel.NONE.value
    y_ordinal_levels[weak_idx] = OrdinalPrecipitationLevel.WEAK.value
    y_ordinal_levels[strong_idx] = OrdinalPrecipitationLevel.STRONG.value
    y_ordinal_levels[moderate_idx] = OrdinalPrecipitationLevel.MODERATE.value
    y_ordinal_levels[extreme_idx] = OrdinalPrecipitationLevel.EXTREME.value
    return y_ordinal_levels

def value_to_binary_level(y):
    none_idx, weak_idx, moderate_idx, strong_idx, extreme_idx = get_events_per_level(y)
    y_levels = np.zeros_like(y)
    y_levels[none_idx] = BinaryPrecipitationLevel.NO_RAIN.value
    y_levels[weak_idx] = y_levels[strong_idx] = y_levels[moderate_idx] = y_levels[extreme_idx] = BinaryPrecipitationLevel.RAIN.value
    return y_levels

def binary_encoding_to_level(y_encoded):
    """
    Converts a numpy array of binary one-hot-encoded values to their corresponding labels.

    For example:
    one_hot_array = np.array([[1, 0], [0, 1], [0, 1]])
    binary_labels = binary_encoding_to_level(one_hot_array)
    print(binary_labels)

    This will output:
    [0, 1, 1]
    """
    binary_labels = []
    for row in y_encoded:
        binary_labels.append(np.argmax(row))
    return binary_labels

def get_events_per_level(y_values):
    assert np.all((y_values >= 0)) # We can't have negative precipitation values...right!?
    thresholds = multiclass_classification_thresholds_dict
    no_rain = np.where(y_values <= thresholds["NO_RAIN"][1])
    weak_rain = np.where((y_values > thresholds["WEAK_RAIN"][0]) & (y_values <= thresholds["WEAK_RAIN"][1]))
    moderate_rain = np.where((y_values > thresholds["MODERATE_RAIN"][0]) & (y_values <= thresholds["MODERATE_RAIN"][1]))
    strong_rain = np.where((y_values > thresholds["STRONG_RAIN"][0]) & (y_values <= thresholds["STRONG_RAIN"][1]))
    extreme_rain = np.where((y_values > thresholds["EXTREME_RAIN"][0]))    
    return no_rain, weak_rain, moderate_rain, strong_rain, extreme_rain
