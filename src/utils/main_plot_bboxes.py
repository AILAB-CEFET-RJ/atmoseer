import folium
import argparse
from geopy.distance import geodesic

'''
    Each bounding box is placed in a separate folium.FeatureGroup, ensuring that overlapping or nested bounding boxes are displayed correctly. 
    Additionally, a layer control is added so users can toggle bounding boxes on and off.
'''
def plot_map(bboxes, colors):
    # Compute center of the first bounding box
    first_bbox = bboxes[0]
    center_lat = (first_bbox[0] + first_bbox[2]) / 2
    center_lon = (first_bbox[1] + first_bbox[3]) / 2
    
    # Create a Folium map centered at the computed location
    m = folium.Map(location=[center_lat, center_lon], zoom_start=8)
    
    # Create separate feature groups for each bounding box
    for i, (lat_min, lon_min, lat_max, lon_max) in enumerate(bboxes):
        color = colors[i % len(colors)]  # Cycle through colors if fewer than bboxes
        bbox_coords = [
            [lat_min, lon_min],
            [lat_min, lon_max],
            [lat_max, lon_max],
            [lat_max, lon_min],
            [lat_min, lon_min]  # Close the polygon
        ]
        
        # Calculate distances of the sides
        width_km = geodesic((lat_min, lon_min), (lat_min, lon_max)).km
        height_km = geodesic((lat_min, lon_min), (lat_max, lon_min)).km
        popup_text = f"Width: {width_km:.2f} km\nHeight: {height_km:.2f} km"
        
        # Create a feature group for the bounding box
        feature_group = folium.FeatureGroup(name=f"Bounding Box {i+1}")
        folium.Polygon(bbox_coords, color=color, weight=3, fill=True, fill_opacity=0.2, popup=popup_text).add_to(feature_group)
        feature_group.add_to(m)
    
    # Add layer control to toggle visibility
    folium.LayerControl().add_to(m)
    
    # Save map to an HTML file
    map_filename = "map.html"
    m.save(map_filename)
    print(f"Map saved to {map_filename}")

def main():
    parser = argparse.ArgumentParser(description="Plot a map with multiple bounding boxes using Folium.")
    parser.add_argument("bboxes", type=float, nargs='+', help="Bounding box coordinates (lat_min lon_min lat_max lon_max) repeated.")
    parser.add_argument("--colors", type=str, nargs='+', default=["red"], help="List of colors for bounding boxes.")
    args = parser.parse_args()
    
    if len(args.bboxes) % 4 != 0:
        raise ValueError("Each bounding box must have exactly four values: lat_min lon_min lat_max lon_max")
    
    # Convert list to tuples of bounding boxes
    bboxes = [tuple(args.bboxes[i:i+4]) for i in range(0, len(args.bboxes), 4)]
    plot_map(bboxes, args.colors)

if __name__ == "__main__":
    main()
# Example usage:
# python src/plot_bboxes.py -23.1339033365138 -43.8906028271505 -22.649724748272934 -43.04835145732227 -23.801876626302175 -45.05290312102409 -21.699774257353113 -42.35676996062447 --colors blue green