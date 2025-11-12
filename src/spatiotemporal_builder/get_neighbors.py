import bisect

import numpy as np
import numpy.typing as npt


def get_upper_neighbor(lat: float, lon: float, sorted_latitudes_ascending: npt.NDArray[np.float32]):
    lat_idx = bisect.bisect_right(sorted_latitudes_ascending, lat)
    if lat_idx < len(sorted_latitudes_ascending):
        return sorted_latitudes_ascending[lat_idx], lon
    return None


def get_bottom_neighbor(
    lat: float, lon: float, sorted_latitudes_ascending: npt.NDArray[np.float32]
):
    lat_idx = bisect.bisect_left(sorted_latitudes_ascending, lat)
    if lat_idx > 0:
        return sorted_latitudes_ascending[lat_idx - 1], lon
    return None


def get_left_neighbor(lat: float, lon: float, sorted_longitudes_ascending: npt.NDArray[np.float32]):
    lon_idx = bisect.bisect_left(sorted_longitudes_ascending, lon)
    if lon_idx > 0:
        return lat, sorted_longitudes_ascending[lon_idx - 1]
    return None


def get_right_neighbor(
    lat: float, lon: float, sorted_longitudes_ascending: npt.NDArray[np.float32]
):
    lon_idx = bisect.bisect_right(sorted_longitudes_ascending, lon)
    if lon_idx < len(sorted_longitudes_ascending):
        return lat, sorted_longitudes_ascending[lon_idx]
    return None
