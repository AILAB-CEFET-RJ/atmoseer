from PIL import Image, ImageDraw
import pandas as pd
from pathlib import Path

def list_files_pathlib(path=Path('.'), arq = []):
    for entry in path.iterdir():
        if entry.is_file():
            tst = Image.open('.\\'+ str(entry))

            draw = ImageDraw.Draw(tst)
            draw.circle((tst.width/2,tst.height/2),2,"black")
            
            pos_sumare = (-22.955139,-43.248278)
            pos_sumare_img = (tst.height/2, tst.width/2)

            dify = (pos_sumare_img[0]/pos_sumare[0])
            difx = (pos_sumare_img[1]/pos_sumare[1])

            df_estacoes = pd.read_csv('./data/estacoes_pluviometricas.csv')

            #Estações COR
            df_estacoes_pluviometricas = df_estacoes[~df_estacoes['N'].isin([1,11,16,19,20,22,28,32])]
            df_estacoes_meteorologicas = df_estacoes[df_estacoes['N'].isin([1,11,16,19,20,22,28,32])]


            #Estações INMET
            dfnew = pd.read_json('https://apitempo.inmet.gov.br/estacoes/T')
            df_estacoes_INMET = dfnew[dfnew['SG_ESTADO'] == 'RJ']

            inmet_oeste = ['A636','A621','A602']
            inmet_sul = ['A652']

            #Desenhar as estações pluviométricas do COR em vermelho
            lat = list(df_estacoes_pluviometricas.Latitude)
            lon = list(df_estacoes_pluviometricas.Longitude)

            for loc in zip(lat, lon):
                posx = pos_sumare[1] - ((loc[1] - pos_sumare[1]) * 22)
                valorx = posx * difx

                posy = pos_sumare[0] + ((loc[0] - pos_sumare[0]) * 22)
                valory = posy * dify
                draw.circle((valorx,valory),1.5,"red")


            #Desenhar as estações meteorológicas do COR em verde
            lat = list(df_estacoes_meteorologicas.Latitude)
            lon = list(df_estacoes_meteorologicas.Longitude)

            for loc in zip(lat, lon):
                posx = pos_sumare[1] - ((loc[1] - pos_sumare[1]) * 22)
                valorx = posx * difx

                posy = pos_sumare[0] + ((loc[0] - pos_sumare[0]) * 22)
                valory = posy * dify
                draw.circle((valorx,valory),1.5,"green")

            #Desenhar as estações meteorológicas do INMET em roxo
            lat = list(df_estacoes_INMET.VL_LATITUDE)
            lon = list(df_estacoes_INMET.VL_LONGITUDE)

            for loc in zip(lat, lon):
                posx = pos_sumare[1] - ((loc[1] - pos_sumare[1]) * 22)
                valorx = posx * difx

                posy = pos_sumare[0] + ((loc[0] - pos_sumare[0]) * 22)
                valory = posy * dify
                draw.circle((valorx,valory),1.5,"purple")

            lat = [-22.81]
            lon = [-43.25]

            for loc in zip(lat, lon):
                posx = pos_sumare[1] - ((loc[1] - pos_sumare[1]) * 22)
                valorx = posx * difx

                posy = pos_sumare[0] + ((loc[0] - pos_sumare[0]) * 22)
                valory = posy * dify
                draw.circle((valorx,valory),1.5,"yellow")

            #width, height = tst.size   # Get dimensions
            #new_width = new_height =656
            #left = (width - new_width)/2 -4
            #top = (height - new_height)/2 +1
            #right = (width + new_width)/2 -4
            #bottom = (height + new_height)/2 -1

            # Crop the center of the image
            #tst2 = tst.crop((left, top, right, bottom))
            tst.save('.\\'+ str(entry))
        elif entry.is_dir():
            list_files_pathlib(entry, arq)

# Specify the directory path you want to start from

arq1 = []
directory_path = Path('./data/radar_sumare/')
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