import os, argparse
import pandas as pd
import cv2
import math

coords = "..\\data\\nova_base\\estacoes_pluviometricas.csv"
coords = pd.read_csv(coords)

latitude_pc = 4.4997
longitude_pc = 4.9321


def gera_df_est():
    
    df_est = pd.DataFrame()

    for n in coords['N']:
 
     e = coords.iloc[(n-1),1]
     lat = coords.iloc[(n-1),2]
     long =coords.iloc[(n-1),3]

     ya, xa = pontos(long,lat)
     xa, ya = round(xa), round(ya)

     d1 = {"Estação": [e], "X": [xa], "Y": [ya]}
     df = pd.DataFrame(d1)
     df_est = pd.concat([df_est,df])
  
    return df_est

def pontos(x,y):
    
    lat_pc, long_pc = -22.464278, -43.297476

    dist_x =  (x - long_pc) 
    dist_y = (y - lat_pc)

    x1 = 125 + (dist_x * 250 / longitude_pc)
    y1 = 125 - (dist_y * 250 / latitude_pc)

    return y1,x1
    
def parameter(image,raio,df_est, df_estacoes):
   
   i = 5  #COPACABANA
   
   x = int(df_est.iloc[i, 1])
   y = int(df_est.iloc[i, 2])
    
   pixel_est = f'({str(df_est.iloc[i, 1])},{str(df_est.iloc[i, 2])})'
   dic1= {'Estação': df_est.iloc[i, 0], 'Pixel': [pixel_est]}
   df_head = pd.DataFrame(dic1)

   dataframe1 = gera_dataframe(image,x,y,raio, df_head, df_estacoes)
   return dataframe1
   


def gera_dataframe(image,x,y,raio,df_head, df_estacoes):  
     
   df_data = pd.DataFrame() 

   for ix in range (x-raio, x+raio+1):
    for  iy in range (y-raio, y+raio+1):

        dist_x = ix - x
        if dist_x > 0:
          coord_x = f"p{dist_x}"
        elif dist_x < 0:
          dist_x = abs(dist_x)
          coord_x = f"m{dist_x}"
        else:
          coord_x = "e0"

        dist_y = iy - y

        if dist_y < 0:
          dist_y = abs(dist_y)
          coord_y = f"p{dist_y}"
        elif dist_y > 0:
          dist_y = (dist_y)
          coord_y = f"m{dist_y}"
        else:
          coord_y = "e0"

        pixel = image[iy, ix]
        B = pixel[0]
        G = pixel[1]
        R = pixel[2]

        hsv_img = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
         
        pixel = hsv_img[iy, ix]
        H = pixel[0]
        S = pixel[1]
        V = pixel[2]

       
   
        dic = {f"{coord_x}{coord_y}R": [R], f"{coord_x}{coord_y}G": [G],f"{coord_x}{coord_y}B": [B],
                                       f"{coord_x}{coord_y}H": [H], f"{coord_x}{coord_y}S":[S], f"{coord_x}{coord_y}V":[V]}
           
        d_colors = pd.DataFrame(dic)
        d_colors[f"{coord_x}{coord_y}R"] = R
        d_colors[f"{coord_x}{coord_y}G"] = G
        d_colors[f"{coord_x}{coord_y}B"] = B
        d_colors[f"{coord_x}{coord_y}H"] = H
        d_colors[f"{coord_x}{coord_y}S"] = S
        d_colors[f"{coord_x}{coord_y}V"] = V   
    
        df_data = pd.concat([df_data,d_colors], axis=1)
        dataframe1 = pd.concat([df_head, df_data], axis=1)

   dataframe1 = dataframe1.loc[~dataframe1.index.duplicated(keep='first')]

   return dataframe1

   
       
path = "..\\data\\nova_base\\img\\"
       
print("Iniciando extração de cores")

def processing(raio):
  
  df_estacoes = pd.DataFrame()

  for year_folder in os.listdir(path):
     year_path = os.path.join(path, year_folder)
     if os.path.isdir(year_path):
              
        for month_folder in os.listdir(year_path):
           month_path = os.path.join(year_path, month_folder)
           if os.path.isdir(month_path):

              for day_folder in os.listdir(month_path):
                 day_path = os.path.join(month_path, day_folder)
                 if os.path.isdir(day_path): 

                    for filename in os.listdir(day_path):
                        if filename.endswith(".png"):
                            
                          file = os.path.join(day_path, filename)

                          image = cv2.imread(file)
                          if image is None:
                              continue
                          image = cv2.resize(image, (250, 250))
                          if image is None:
                              continue

                          df_est = gera_df_est()

                          dataframe1 = parameter(image,raio, df_est, df_estacoes)
                          dataframe1.insert(0, "date", filename.split(".")[0])
                          df_estacoes = pd.concat([df_estacoes, dataframe1], ignore_index=True)
                         
  
  df_estacoes.sort_values("date")
  df_estacoes.to_csv(f"..\\data\\nova_base\\FEATURE_A652_COLORCORD.csv")

def parameter_parser():
    description ="Script to parametrize one region around a station, represented by a pixel. Analyzes in RGB and HSV colors. \n \
                  acepts [-r, -f] to radius of region and wich feature will be used. \n "
    
    parser = argparse.ArgumentParser(description=description)
    
    parser.add_argument('-r',"--radius", required=False)
  

    return parser.parse_args()

if __name__ == '__main__':
    
    args = parameter_parser()

    raio = args.radius if args.radius != None else 1
    raio = int(raio)
    processing(raio)



