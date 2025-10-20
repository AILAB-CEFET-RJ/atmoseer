from PIL import Image, ImageDraw
import pandas as pd
from pathlib import Path


def list_files_pathlib(path=Path('.'), arq = []):
    for entry in path.iterdir():
        if entry.is_file():
            try:
                tst = Image.open(str(entry))

                width, height = tst.size   # Get dimensions
                new_width = new_height =656
                left = (width - new_width)/2 -4
                top = (height - new_height)/2 +1
                right = (width + new_width)/2 -4
                bottom = (height + new_height)/2 -1

                # Crop the center of the image
                tst2 = tst.crop((left, top, right, bottom))
                
                tst2.save(str(entry))
                print("Cropped file " + str(entry) + " saved")
            except Exception as e:
                print("File not Cropped: " + str(entry))
    
        elif entry.is_dir():
            list_files_pathlib(entry, arq)

# Specify the directory path you want to start from

arq1 = []
directory_path = Path("./data/radar_sumare" \
"")
list_files_pathlib(directory_path, arq1)

print(arq1)
#print(glob.glob("./data/radar_sumare/*")) 

'''
tst = Image.open("2024_01_13_16_37.png")

width, height = tst.size   # Get dimensions
new_width = new_height =656
left = (width - new_width)/2
top = (height - new_height)/2
right = (width + new_width)/2
bottom = (height + new_height)/2

# Crop the center of the image
tst2 = tst.crop((left, top, right, bottom))
tst2'''