
import pandas as pd
import numpy as np
import math
import os
from PIL import Image
import argparse



class RadarSumare:
    """
    Class for reading and interpolating radar reflectivity data
    over the Sumaré region, using PNG radar images.
    """

    def __init__(self, base_path: str = "../../data/radar_sumare/"):
        """
        Initialize the RadarSumare class.

        Args:
            base_path: Base directory where radar images are stored.
                       Defaults to "../../data/radar_sumare/".
        """
        print(base_path)
        self.base_path = base_path

    @staticmethod
    def rgb_distance(c1: tuple[int, int, int], c2: tuple[int, int, int]) -> float:
        """
        Calculates the Euclidean distance between two RGB colors.

        Args:
            c1: First color (tuple of 3 integers)
            c2: Second color (tuple of 3 integers)

        Returns:
            float: Euclidean distance between both colors
        """
        return math.sqrt(
            (c1[0] - c2[0]) ** 2 +
            (c1[1] - c2[1]) ** 2 +
            (c1[2] - c2[2]) ** 2
        )

    @staticmethod
    def interpolate_value(rgb: tuple[int, int, int],
                          legend_colors: list[tuple[int, int, int]],
                          legend_values: list[float]) -> float:
        """
        Interpolates a value associated with an RGB color
        based on a color legend and its corresponding values.

        Args:
            rgb: tuple (R, G, B) of the color obtained from a pixel
            legend_colors: list of RGB colors in the legend
            legend_values: list of values associated with each color

        Returns:
            float: interpolated value corresponding to the input color
        """
        if rgb == (0, 0, 0):
            return 0.0

        # Compute distances between the input color and all legend colors
        distances = [RadarSumare.rgb_distance(rgb, lc) for lc in legend_colors]

        # Identify the two closest colors in the legend
        min_idx1 = distances.index(min(distances))
        distances[min_idx1] = float('inf')  # ignore the closest to find the second
        min_idx2 = distances.index(min(distances))

        # Retrieve corresponding colors and values
        c1, c2 = legend_colors[min_idx1], legend_colors[min_idx2]
        v1, v2 = legend_values[min_idx1], legend_values[min_idx2]

        # Compute interpolation weight
        dist_c1_c2 = RadarSumare.rgb_distance(c1, c2)
        dist_c1_rgb = RadarSumare.rgb_distance(c1, rgb)
        t = dist_c1_rgb / dist_c1_c2 if dist_c1_c2 != 0 else 0

        # Interpolate the reflectivity value
        interpolated_value = v1 + t * (v2 - v1)
        return interpolated_value

    def get_radar_data(self, start: str, end: str, frequency: str,
                       latitude: float, longitude: float) -> pd.DataFrame:
        """
        Retrieves radar reflectivity data for a given time range and location.

        Args:
            start: start date in 'YYYY-MM-DD' format
            end: end date in 'YYYY-MM-DD' format
            frequency: time step between images (e.g., '10min')
            latitude: latitude of the target point
            longitude: longitude of the target point

        Returns:
            pd.DataFrame: DataFrame with ['datahora', 'reflect']
        """
        latitude = float(latitude)
        longitude = float(longitude)

        # Reference coordinates for Sumaré radar center
        pos_sumare = (-22.955139, -43.248278)

        # Color scale (legend) for reflectivity intensity
        legend_values = [50, 45, 40, 35, 30, 25, 20, 0]
        legend_colors = [
            (197, 0, 197),   # Magenta
            (227, 6, 5),     # Red
            (255, 112, 0),   # Orange
            (195, 230, 0),   # Yellow
            (4, 85, 4),      # Yellow-green
            (19, 122, 19),   # Dark green
            (0, 167, 12),    # Light green
            (0, 0, 0)        # Empty / no data
        ]

        # Build date range
        start_date = start + " 00:00:00"
        end_date = end + " 23:58:00"
        dates = pd.date_range(start=start_date, end=end_date, freq=frequency)

        radar_data = pd.DataFrame({"datahora": dates})
        radar_data["reflect"] = np.nan

        for count in range(len(radar_data)):
            datetime_str = radar_data.iloc[count]["datahora"].strftime("%Y-%m-%d %H:%M:%S")

            year = datetime_str[0:4]
            month = datetime_str[5:7]
            day = datetime_str[8:10]
            hour = datetime_str[11:13]
            minute = datetime_str[14:16]
            file = f"{year}_{month}_{day}_{hour}_{minute}.png"
            file_path = os.path.join(self.base_path, year, month, day, file)

            if os.path.exists(file_path):
                try:
                    img = Image.open(file_path)
                    pos_sumare_img = (img.height / 2, img.width / 2)

                    # Compute coordinate conversion ratios
                    dify = (pos_sumare_img[0] / pos_sumare[0])
                    difx = (pos_sumare_img[1] / pos_sumare[1])

                    # Compute target pixel position based on lat/lon
                    posx = pos_sumare[1] - ((longitude - pos_sumare[1]) * 32.5)
                    valorx = posx * difx
                    posy = pos_sumare[0] + ((latitude - pos_sumare[0]) * 19.5)
                    valory = posy * dify

                    # Extract pixel color and interpolate reflectivity value
                    rgb_im = img.convert("RGB")
                    r, g, b = rgb_im.getpixel((valorx, valory))
                    radar_data.at[count, "reflect"] = RadarSumare.interpolate_value(
                        (r, g, b), legend_colors, legend_values
                    )
                    print(f"Image {file} successfully processed")
                except Exception as e:
                    print(f"Error processing image {file}: {e}")

        return radar_data

def parse_arguments():
    parser = argparse.ArgumentParser(description="Baixa dados meteorológicos de refletividade do Radar Sumaré")
    parser.add_argument("--inicio", help="Data inicial no formato YYYY-MM-DD", default=None)
    parser.add_argument("--fim", help="Data final no formato YYYY-MM-DD", default=None)
    parser.add_argument("--frequencia", help="Escala Temporal a ser demandada", default="2min")
    parser.add_argument("--latitude", required=True, help="Latitude do ponto solicitado")
    parser.add_argument("--longitude", required=True, help="Longitude do ponto solicitado")

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    radar_data = RadarSumare.get_radar_data(args.inicio, 
                                args.fim, 
                                args.frequencia, 
                                args.latitude, 
                                args.longitude)
    radar_data.to_parquet("./data/radar_sumare/radar_data.parquet")
