import pandas as pd
import folium

def parse_coordinates(coord_str):
    
    degrees = float(coord_str.split('º')[0])
    minutes = float(coord_str.split('º')[1].split("'")[0])
    seconds = float(coord_str.split("'")[1].split("''")[0])
    direction = coord_str.split("'' ")[1]
    
    decimal_degrees = degrees + minutes / 60 + seconds / 3600
    if direction in ['S', 'W']:
        decimal_degrees = -decimal_degrees
    
    return decimal_degrees

def add_points_to_map(map_obj: folium.Map, df: pd.DataFrame, radius=5, color='blue', fill_opacity=0.6):
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['lat'], row['long']],
            radius=radius,
            color=color,
            fill_color=color,
            fill_opacity=fill_opacity,
            popup=f"{row['nome']}: {row['lat']}, {row['long']}",
        ).add_to(map_obj)
    return map_obj

redemet_df = pd.DataFrame({
    'nome': ['SBGL', 'SBJR', 'SBRJ', 'SBSC', 'SBAF'],
    'lat': ["22º48'32'' S", "22º59'12'' S", "22º54'37'' S", "22º55'56'' S", "22º52'30'' S"],
    'long': ["43º14'37'' W", "43º22'19'' W", "43º9'47'' W", "43º43'8'' W", "43º 23'4'' W"]
})


redemet_df['lat'] = redemet_df['lat'].apply(parse_coordinates)
redemet_df['long'] = redemet_df['long'].apply(parse_coordinates)

centroid = [-22.89181404239337, -43.269477142236386]
zoom_start = 11
tiles = "cartodbpositron"

_map = folium.Map(
    location=centroid,
    zoom_start=zoom_start,
    tiles=tiles,
)

_map = add_points_to_map(
    _map,
    redemet_df,
    color='darkgreen',
)

_map.save('mapa.html')

_map

cemaden_df = pd.read_csv("cemaden2.csv")
cemaden_df.tail()
print(cemaden_df)

cemaden_df = pd.read_csv("cemaden2.csv", usecols=['nome', 'lat', 'long'])
cemaden_df.columns = ['nome', 'lat', 'long']

cemaden_df.head()



required_columns = ['lat', 'long']
dataframes = [redemet_df, cemaden_df]

for df in dataframes:
    for column in required_columns:
        assert column in df.columns, f"Column '{column}' not found in DataFrame '{df}'"


_map =folium.Map(
    location=centroid, 
    zoom_start=zoom_start,
    tiles=tiles,
)

_map = add_points_to_map(
    _map, 
    redemet_df,
    color='darkred', 
)

_map = add_points_to_map(
    _map, 
    cemaden_df,
    color='darkblue', 
)

colors = ['red', 'blue']
labels = ['Redemet', 'Cemaden']

legend_html = '''
<div style="position: fixed; 
    bottom: 1px; right: 1px; width: 120px; height: 80px; 
    border:2px solid grey; z-index:9999; font-size:14px;
    background-color: white;
">
'''

for i, label in enumerate(labels):
    legend_html += f'''
    &nbsp;&nbsp;&nbsp;<i class="fa fa-circle fa-1x" style="color:{colors[i]}"></i>&nbsp;{label}<br>
'''
legend_html += '</div>'

_map.get_root().html.add_child(folium.Element(legend_html))
_map

_map.save('mapaoficial.html')
_map
