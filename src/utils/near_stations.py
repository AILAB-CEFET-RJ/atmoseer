from math import cos, asin, sqrt
import pandas as pd

def myFunc2(e):
  return e[:][1]

def prox(nome, num):
  lugar = []
  result = []
  aux1 = pd.read_csv('../data/estacoes_local.csv')
  aux = aux1[~aux1['files'].isin([nome])]
  del aux['Unnamed: 0']
  for loc in aux['DC_NOME']:
      p = 0.017453292519943295
      alvo = aux1[aux1['files'] == nome]
      est = aux[aux['DC_NOME'] == loc]

      hav = 0.5 - cos((est.VL_LATITUDE.iloc[0]-alvo.VL_LATITUDE.iloc[0])*p)/2 + cos(alvo.VL_LATITUDE.iloc[0]*p)*cos(est.VL_LATITUDE.iloc[0]*p) * (1-cos((est.VL_LONGITUDE.iloc[0]-alvo.VL_LONGITUDE.iloc[0])*p)) / 2
      
      dist = 12742 * asin(sqrt(hav))
      lugar = lugar + [(est.files.iloc[0],dist)]
      lugar.sort(key=myFunc2) 
      lugar = lugar[0:num]  
      result = [i[0] for i in lugar]
  print(result)
  return result