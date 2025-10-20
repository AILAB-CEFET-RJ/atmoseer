import requests, os, time
import datetime
from datetime import datetime, timedelta
import argparse
import shutil
import pandas as pd
import urllib.request

api_key = "aOrV6N5DtQen69uEDlYwbUTo3CTTCcAZ50r7WEW4"

def extract(datas_str):
       
    dados_faltantes = []
    sem_dados = []

    for data in datas:

            print(f"\n{data}")

            date_h = []
            for hora in range(0, 24):
             date_h.append(data.replace(hour=hora))
            datas_str = [data.strftime("%Y%m%d%H") for data in date_h]

            print('iniciando requisição')

            true = []

            for i in datas_str:
             
             requisicao = requests.get(f'https://api-redemet.decea.mil.br/produtos/radar/03km?api_key={api_key}&radar=pc&anima=3&data={i}')
             #time.sleep(0.2)
            
             print(f"Requisição concluida da hora {i}")      
         
             if "Server Error" not in requisicao.text:
              json = requisicao.json()
              if "status" in json:
                if json["status"] == True:
                 true.append(json)
                
                elif "error" in json:
                  print("OVER_RATE LIMIT")
                  break
                else:
                 print("Sem status")
              else:
                print("ERRO DE SERVIDOR")

            radares = [i["data"]["radar"] for i in true]
            rad1 = [i[0] for i in radares]
            rad2 = [i[1] for i in radares]
            rad3 = [i[2] for i in radares]
            radares_1 = rad1 + rad2 + rad3

        
            date1 = [i[11]["data"] for i in radares_1]
            date_t = [x for x in date1 if x is not None]
            date_true = [datetime.strptime(i, '%Y-%m-%d %H:%M:%S') for i in date_t]
            date_days = [i.strftime('%Y%m%d%H') for i in date_true]
            date_days = list(set(date_days))
            hora_sem_retorno = list(set(datas_str) - set(date_days))

            if len(hora_sem_retorno) == 24:

               sem_dados.append(list(i for i in hora_sem_retorno))
               print("Atenção: DATA SEM DADOS!")
               time.sleep(0.2)

            elif len(hora_sem_retorno) < 24 and len(hora_sem_retorno) > 0:
               
                dados_faltantes.append(list(i for i in hora_sem_retorno))
                print(f"Atenção: data com horas faltantes. ({len(hora_sem_retorno)})")
        
            elif len(hora_sem_retorno) == 0:
               print("Data com todos os dados")  

            pc = [i[11]["path"] for i in radares_1]
            path_pc = list(set(pc))
            path_pc = [i for i in path_pc if type(i) is str]

        
            for i in path_pc:
                filename = i.split("/")[-1]
                response = requests.get(i)
                with open("..\\data\\nova_base\\img\\"+filename.split(".")[0].replace(":","")+".png", 'wb') as f:
                    f.write(response.content)

            print("\nDados Baixados\n")

def parameter_parser():
    description = 'Script to perform ETL on PICODOCOUTO data. Format: DD/MM/YYYY'

    parser = argparse.ArgumentParser(description=description)
    
    parser.add_argument('-b',"--dt_begin", required=True)
    
    parser.add_argument('-e', "--dt_end", required=True)

    return parser.parse_args()

args = parameter_parser()

db = args.dt_begin[:2]
mb = args.dt_begin[3:5]
yb = args.dt_begin[6:10]

de = args.dt_end[:2]
me = args.dt_end[3:5]
ye = args.dt_end[6:10]

print(db,mb,yb)

data_inicial = datetime(int(yb), int(mb), int(db))
data_final = datetime(int(ye), int(me), int(de))

datas = []
data_atual = data_inicial
while data_atual <= data_final:
    datas.append(data_atual)
    data_atual += timedelta(days=1)

extract(datas)

dir_path = "..\\data\\nova_base\\img"

for filename in os.listdir(dir_path):
    if filename.endswith(".png"):
        datetime_str = filename.split("--")[0]
        datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d")
        year_dir = os.path.join(dir_path, datetime_obj.strftime("%Y"))
        if not os.path.exists(year_dir):
            os.mkdir(year_dir)
        month_dir = os.path.join(year_dir, datetime_obj.strftime("%m"))
        if not os.path.exists(month_dir):
            os.mkdir(month_dir)
        day_dir = os.path.join(month_dir, datetime_obj.strftime("%d"))
        if not os.path.exists(day_dir):
            os.mkdir(day_dir)
        src_path = os.path.join(dir_path, filename)
        dst_path = os.path.join(day_dir, filename)
        shutil.move(src_path, dst_path)