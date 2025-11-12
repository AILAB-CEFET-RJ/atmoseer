from typing import Optional

import numpy as np
import numpy.typing as npt
from pydantic import BaseModel

from .get_neighbors import get_bottom_neighbor, get_right_neighbor, get_upper_neighbor


class Square(BaseModel):
    top_left: tuple[float, float]
    bottom_left: tuple[float, float]
    bottom_right: tuple[float, float]
    top_right: tuple[float, float]


def get_square(
    lat: float,
    lon: float,
    sorted_latitudes_ascending: npt.NDArray[np.float32],
    sorted_longitudes_ascending: npt.NDArray[np.float32],
) -> Optional[Square]:
    """
    Get the square that contains the point (lat, lon)
    Example, given this grid:
          0   1   2   3
        0 *   *   *   *
        1 *   *   *   *
        2 *   *   *   *
    Given that lat long "*" is the top_left = (0,0):
    top_left's bottom_left neighbor = (1,0)
    bottom_left's bottom_right neighbor = (1,1)
    bottom_right's top_right neighbor = (0,1)

    With top_left, bottom_left, bottom_right, top_right we can create a square

    Note we can get out of bounds, that's when we return None.
    For example, there's no bottom neighbor for (3,3)
    """
    bottom_neighbor = get_bottom_neighbor(lat, lon, sorted_latitudes_ascending)
    if bottom_neighbor is None:
        return None
    lat_bottom, lon_bottom = bottom_neighbor

    right_neighbor = get_right_neighbor(lat_bottom, lon_bottom, sorted_longitudes_ascending)
    if right_neighbor is None:
        return None
    lat_right, lon_right = right_neighbor

    upper_neighbor = get_upper_neighbor(lat_right, lon_right, sorted_latitudes_ascending)
    if upper_neighbor is None:
        return None
    lat_upper, lon_upper = upper_neighbor

    return Square(
        top_left=(lat, lon),
        bottom_left=(lat_bottom, lon_bottom),
        bottom_right=(lat_right, lon_right),
        top_right=(lat_upper, lon_upper),
    )
