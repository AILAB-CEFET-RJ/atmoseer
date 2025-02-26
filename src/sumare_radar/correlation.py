
import pandas as pd
import numpy as np
import math
from scipy import stats
import statsmodels.api as sm
import statsmodels.formula.api as smf
import sys, getopt, os, re
from PIL import Image, ImageDraw

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


def get_radar_data(station_name, met):
    df_local = pd.read_csv('./WeatherStations.csv') 
    df_local = df_local[df_local['STATION_ID'] == station_name]
    
    if station_name in STATION_NAMES_FOR_RJ:
        if met:
            df = pd.read_csv('./data/landing/'+station_name + '.csv')
        else:
            df = pd.read_csv('./data/landing/plv/'+station_name + '.csv')
    else:
        df = pd.read_parquet('./data/inmet-data/'+station_name + '.parquet')
    df['reflect'] = np.nan
    vlat = df_local['VL_LATITUDE'].iloc[0]
    vlon = df_local['VL_LONGITUDE'].iloc[0]
    print(station_name)
    i = 0
    
    while i < len(df):
        df1 = df[df.index == i]
        if station_name in STATION_NAMES_FOR_RJ:
            arquivo = df1['Dia'].iloc[0][:4] +'_'+ df1['Dia'].iloc[0][5:7] +'_'+ df1['Dia'].iloc[0][8:10] + '_' +  df1['Hora'].iloc[0][:2] + '_' +  df1['Hora'].iloc[0][3:5]
            caminho = './data/radar_sumare/'+ df1['Dia'].iloc[0][:4] +'/'+ df1['Dia'].iloc[0][5:7] +'/'+ df1['Dia'].iloc[0][8:10] +'/' + arquivo + '.png'
        else:
            arquivo = df1['DT_MEDICAO'].iloc[0][:4] +'_'+ df1['DT_MEDICAO'].iloc[0][5:7] +'_'+ df1['DT_MEDICAO'].iloc[0][8:10] + '_' + str(df1['HR_MEDICAO'].iloc[0]).zfill(4)[:2] + '_' +  str(df1['HR_MEDICAO'].iloc[0]).zfill(4)[2:4]
            caminho = './data/radar_sumare/'+ df1['DT_MEDICAO'].iloc[0][:4] +'/'+ df1['DT_MEDICAO'].iloc[0][5:7] +'/'+ df1['DT_MEDICAO'].iloc[0][8:10] +'/'+arquivo+'.png'
        if os.path.exists(caminho):
            img = Image.open(caminho)
            pos_sumare = (-22.955139,-43.248278)
            pos_sumare_img = (img.height/2, img.width/2)

            dify = (pos_sumare_img[0]/pos_sumare[0])
            difx = (pos_sumare_img[1]/pos_sumare[1])

            posx = pos_sumare[1] - ((vlon - pos_sumare[1]) * 32.5)
            valorx = posx * difx
            posy = pos_sumare[0] + ((vlat - pos_sumare[0]) * 19.5)
            valory = posy * dify
            
            rgb_im = img.convert('RGB')
            r, g, b = rgb_im.getpixel((valorx, valory))
            #print(r, g, b)
            df.at[i,'reflect'] = interpolate_value((r, g, b),legend_colors,legend_values)
        else:
            arquivo = df1['Dia'].iloc[0][:4] +'_'+ df1['Dia'].iloc[0][5:7] +'_'+ df1['Dia'].iloc[0][8:10] + '_' +  df1['Hora'].iloc[0][:2] + '_' + str(int(df1['Hora'].iloc[0][3:5]) + 1).zfill(2)
            caminho = './data/radar_sumare/'+ df1['Dia'].iloc[0][:4] +'/'+ df1['Dia'].iloc[0][5:7] +'/'+ df1['Dia'].iloc[0][8:10] +'/' + arquivo + '.png'
            if os.path.exists(caminho):
                img = Image.open(caminho)
                pos_sumare = (-22.955139,-43.248278)
                pos_sumare_img = (img.height/2, img.width/2 -1)

                dify = (pos_sumare_img[0]/pos_sumare[0])
                difx = (pos_sumare_img[1]/pos_sumare[1])

                posx = pos_sumare[1] - ((vlon - pos_sumare[1]) * 32.5)
                valorx = posx * difx
                posy = pos_sumare[0] + ((vlat - pos_sumare[0]) * 19.5)
                valory = posy * dify
                
                rgb_im = img.convert('RGB')
                r, g, b = rgb_im.getpixel((valorx, valory))
                #print(r, g, b)
                df.at[i,'reflect'] = interpolate_value((r, g, b),legend_colors,legend_values)
        i = i + 1
    #print(df['reflect'].unique())
    df_correlacao = df[df['reflect'].notnull()]

    if station_name in STATION_NAMES_FOR_RJ:
        if met:
            df_correlacao.loc[df_correlacao.index.values,'Chuva'] = df_correlacao['Chuva'].fillna(0)
            if df['reflect'].unique().size == 1  or df['reflect'].unique().size == 2 :
                print('Não possui nenhum registro de reflectividade')
            else:
                print(stats.spearmanr(df_correlacao['Chuva'],df_correlacao['reflect']))
                print(stats.pearsonr(df_correlacao['Chuva'],df_correlacao['reflect']))
        else:
            df_correlacao.loc[df_correlacao.index.values,'15 min'] = df_correlacao['15 min'].fillna(0)
            if df['reflect'].unique().size == 1  or df['reflect'].unique().size == 2 :
                print('Não possui nenhum registro de reflectividade')
            else:
                print(stats.spearmanr(df_correlacao['15 min'],df_correlacao['reflect']))
                print(stats.pearsonr(df_correlacao['15 min'],df_correlacao['reflect']))
    else:
        df_correlacao.loc[df_correlacao.index.values,'CHUVA'] = df_correlacao['CHUVA'].fillna(0)
        if df['reflect'].unique().size == 1  or df['reflect'].unique().size == 2 :
            print('Não possui nenhum registro de reflectividade')
        else:
            print(stats.spearmanr(df_correlacao['CHUVA'],df_correlacao['reflect']))
            print(stats.pearsonr(df_correlacao['CHUVA'],df_correlacao['reflect']))

    

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
legend_values = [50, 45, 40, 35, 30, 25, 20,0]

#STATION_NAMES_FOR_RJ = ('alto_da_boa_vista', 'guaratiba', 'iraja',
#                        'jardim_botanico', 'riocentro', 'santa_cruz',
#                        'sao_cristovao', 'vidigal')

STATION_NAMES_FOR_RJ = ('alto_da_boa_vista','guaratiba',
                        'iraja','jardim_botanico',
                        'riocentro','santa_cruz',
                        'sao_cristovao','vidigal',
                        'urca','rocinha',
                        'tijuca','santa_teresa',
                        'copacabana','grajau',
                        'ilha_do_governador',
                        'penha','madureira',
                        'bangu','piedade',
                        'tanque','saude',
                        'barrinha','cidade_de_deus',
                        'grajau','grande_meier',
                        'anchieta','grota_funda',
                        'campo_grande','sepetiba',
                        'av_brasil_mendanha','recreio',
                        'laranjeiras','tijuca_muda')

for station in STATION_NAMES_FOR_RJ :
    get_radar_data(station, False)

#get_radar_data('tijuca_muda',False)