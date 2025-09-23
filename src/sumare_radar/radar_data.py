
import pandas as pd
import numpy as np
import math
import os
from PIL import Image
import argparse


def rgb_distance(c1, c2):
    return math.sqrt((c1[0] - c2[0])**2 + (c1[1] - c2[1])**2 + (c1[2] - c2[2])**2)

def interpolate_value(rgb, legend_colors, legend_values):
    if rgb == (0,0,0):
        return 0
    # Calcular distâncias entre a cor fornecida e todas as cores da legenda
    distances = [rgb_distance(rgb, lc) for lc in legend_colors]

    # Identificar as duas cores mais próximas na legenda
    min_idx1 = distances.index(min(distances))  # Índice da cor mais próxima
    distances[min_idx1] = float('inf')  # Excluir a cor mais próxima para achar a segunda
    min_idx2 = distances.index(min(distances))  # Índice da segunda cor mais próxima

    # Obter as cores e valores correspondentes
    c1, c2 = legend_colors[min_idx1], legend_colors[min_idx2]
    v1, v2 = legend_values[min_idx1], legend_values[min_idx2]

    # Calcular o peso de interpolação (t)
    dist_c1_c2 = rgb_distance(c1, c2)
    dist_c1_rgb = rgb_distance(c1, rgb)
    t = dist_c1_rgb / dist_c1_c2 if dist_c1_c2 != 0 else 0

    # Interpolar o valor
    interpolated_value = v1 + t * (v2 - v1)
    return interpolated_value

def get_radar_data(inicio, fim, frequencia, latitude, longitude):
    latitude = float(latitude)
    longitude = float(longitude)

    pos_sumare = (-22.955139,-43.248278)
    legend_values = [50, 45, 40, 35, 30, 25, 20,0]
    legend_colors = [
    (197,0,197),  # Magenta
    (227,6,5),    # Vermelho
    (255,112,0),  # Laranja
    (195,230,0),  # Amarelo
    (4,85,4),  # Amarelo
    (19,122,19),    # Verde escuro
    (0,167,12),     # Verde claro
    (0,0,0)         #Vazio
    ]

    # Definir datas inicial e final
    data_inicial = inicio + " 00:00:00"
    data_final   = fim +  " 23:58:00"

    datas = pd.date_range(start=data_inicial, end=data_final, freq=frequencia)

    # Criar DataFrame com coluna "datahora"
    radar_data = pd.DataFrame({"datahora": datas})
    radar_data['reflect'] = np.nan

    count = 0
    while count < len(radar_data):

        datetime_str  = radar_data.iloc[count]['datahora'].strftime("%Y-%m-%d %H:%M:%S")
      
        year = datetime_str[0:4]
        month = datetime_str[5:7]
        day = datetime_str[8:10]
        hour = datetime_str[11:13]
        minute = datetime_str[14:16]
        file = year + '_'+ month + '_'+ day  + '_' +  hour + '_' + minute + '.png'
        file_path = './data/radar_sumare/' + year + "/" + month + "/" + day + "/" + file

        if os.path.exists(file_path):
            print(file_path)
            try:
                img = Image.open(file_path)
                print(type(img))
                pos_sumare_img = (img.height/2, img.width/2)

                dify = (pos_sumare_img[0]/pos_sumare[0])
                difx = (pos_sumare_img[1]/pos_sumare[1])

                posx = pos_sumare[1] - ((longitude - pos_sumare[1]) * 32.5)
                valorx = posx * difx
                posy = pos_sumare[0] + ((latitude - pos_sumare[0]) * 19.5)
                valory = posy * dify
                
                rgb_im = img.convert('RGB')
                r, g, b = rgb_im.getpixel((valorx, valory))
                radar_data.at[count,'reflect'] = interpolate_value((r, g, b), legend_colors, legend_values)
            except Exception as e:
                print("Mensagem de erro:", str(e))           # como string
   
        count += 1

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
    radar_data = get_radar_data(args.inicio, 
                                args.fim, 
                                args.frequencia, 
                                args.latitude, 
                                args.longitude)
    radar_data.to_parquet("./data/radar_sumare/radar_data.parquet")
