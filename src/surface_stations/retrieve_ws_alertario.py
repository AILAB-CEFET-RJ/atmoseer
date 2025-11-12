import pandas as pd
import numpy as np
import sys, getopt, os, re
import datetime

STATION_NAMES_FOR_RJ = ('alto_da_boa_vista',
                        'guaratiba',
                        'iraja',
                        'jardim_botanico',
                        'riocentro',
                        'santa_cruz',
                        'sao_cristovao',
                        'vidigal')

def corrige_txt(station_name, years, months):
    for year in years:
        for month in months:
            file = "../data/RAW_data/COR/meteorologica/" + station_name + "_" + year + month + "_Met.txt"
            if os.path.exists(file):
                fin = open(file, "rt")
                if not os.path.exists("../data/RAW_data/COR/meteorologica/aux_"+ station_name):
                    os.mkdir("../data/RAW_data/COR/meteorologica/aux_"+ station_name)
                fout = open("../data/RAW_data/COR/meteorologica/aux_" + station_name + "/" + station_name + "_" + year + month + "_Met2.txt", "wt")
                count = 0
                for line in fin:
                    count += 1
                    if station_name == 'guaratiba':
                        fout.write(re.sub('\s+', ' ', line.replace(':40      ', ':00  nHBV')))
                    else:
                        fout.write(re.sub('\s+', ' ', line.replace(':00      ', ':00  nHBV')))
                    fout.write('\n')
                if count == 6:
                    fout.write('01/' + month + '/'+ year + ' 00:00:00 nHBV ND ND ND ND ND ND')
                fin.close()
                fout.close()
            else:
                pass

def import_data(station_name, years, months, arg_end):
    df = pd.DataFrame()

    for year in years:
        check = 0
        for month in months:
            texto = '../data/RAW_data/COR/meteorologica/aux_' + station_name + '/' + station_name + '_' + year + month + '_Met2.txt'
            if os.path.exists(texto):
                df = pd.read_csv(texto, sep=' ', skiprows=[0, 1, 2, 3, 4, 5], header=None)
                if len(df.columns) == 10:
                    df.columns = ['Dia', 'Hora', 'HBV', 'Chuva', 'DirVento','VelVento', 'Temperatura', 'Pressao', 'Umidade', 'teste']
                    del df["teste"]
                else:
                    df.columns = ['Dia', 'Hora', 'HBV', 'Chuva', 'DirVento','VelVento', 'Temperatura', 'Pressao', 'Umidade']
                df['Chuva'] = df['Chuva'][~df['Chuva'].isin(['-', 'ND'])].astype(float)
                df['Umidade'] = df['Umidade'][df['Umidade'] != 'ND'].astype(float)
                df['Temperatura'] = df['Temperatura'][df['Temperatura'] != 'ND'].astype(float)
                df['DirVento'] = df['DirVento'][~df['DirVento'].isin(['-', 'ND'])].astype(float)
                df['VelVento'] = df['VelVento'][df['VelVento'] != 'ND'].astype(float)
                df['Pressao'] = df['Pressao'][df['Pressao'] != 'ND'].astype(float)
                df['Dia'] = pd.to_datetime(df['Dia'], format='%d/%m/%Y')
                ano_aux = year
                mes_aux = month
                print(year + '/' + month)
                check = 1
                break
            else:
                pass
        if check == 1:
            break
        
    ano1 = list(map(str,range(int(ano_aux),arg_end)))
    mes1 = list(range(int(mes_aux),13))
    mes1 = [str(i).rjust(2, '0') for i in mes1]

    for year in ano1:
        for month in mes1:
            texto = '../data/RAW_data/COR/meteorologica/aux_' + station_name + '/' + station_name + '_' + year + month + '_Met2.txt'
            if os.path.exists(texto):
                data2 = pd.read_csv(texto, sep=' ', skiprows=[0, 1, 2, 3, 4, 5], header=None, on_bad_lines='skip')
                if len(data2.columns) == 10:
                    data2.columns = ['Dia', 'Hora', 'HBV', 'Chuva', 'DirVento','VelVento', 'Temperatura', 'Pressao', 'Umidade', 'teste']
                    del data2["teste"]
                else:
                    data2.columns = ['Dia', 'Hora', 'HBV', 'Chuva', 'DirVento','VelVento', 'Temperatura', 'Pressao', 'Umidade']
                data2['Chuva'] = data2['Chuva'][~data2['Chuva'].isin(['-', 'ND'])].astype(float)
                data2['Umidade'] = data2['Umidade'][data2['Umidade'] != 'ND'].astype(float)
                data2['Temperatura'] = data2['Temperatura'][~data2['Temperatura'].isin(['-', 'ND'])].astype(float)
                data2['DirVento'] = data2['DirVento'][~data2['DirVento'].isin(['-', 'ND'])].astype(float)
                data2['VelVento'] = data2['VelVento'][data2['VelVento'] != 'ND'].astype(float)
                data2['Pressao'] = data2['Pressao'][data2['Pressao'] != 'ND'].astype(float)
                data2['Dia'] = pd.to_datetime(data2['Dia'], format='%d/%m/%Y')
                saida = pd.concat([df, data2])
                df = saida
                del saida
            else:
                pass
    if year == ano_aux:
        mes1 = list(range(1,13))
        mes1 = [str(i).rjust(2, '0') for i in mes1]
    df = df.replace('ND', np.NaN)
    df = df.replace('-', np.NaN)
    df = df[df['Hora'].str[2:6] == ':00:']
    df['Hora'] = np.where(df['HBV'] == 'HBV', df['Hora'].str[0:2].astype(int) - 1, df['Hora'].str[0:2].astype(int))
    df['Hora'] = np.where(df['Hora'] == -1, 23, df['Hora'])
    df['Hora'] = df['Hora'].astype(str).str.zfill(2) + ':00:00'

    df['estacao'] = station_name
    df.to_csv('../data/landing/' + station_name + '.csv')
    data_aux = df
    del df, data2
    return data_aux


def import_data(station_name, initial_year, final_year):
    years  = list(map(str,range(initial_year, final_year)))
    months = list(range(1,13))
    months = [str(i).rjust(2, '0') for i in months]

    if station_name == "all":
        station_names = STATION_NAMES_FOR_RJ
    else:
        station_names = [station_name]

    for station_name in station_names:
        corrige_txt(station_name, years, months)
        data = import_data(station_name, years, months, final_year)
        del data

def main(argv):
    station_name = ""
    default_initial_year = 1997

    today = datetime.date.today()
    default_final_year = today.year

    arg_help = "{0} -s <station> -b <begin> -e <end>".format(argv[0])
    
    try:
        opts, args = getopt.getopt(argv[1:], "hs:a:b:e:", ["help", "station=", "begin=", "end="])
    except:
        print(arg_help)
        sys.exit(2)
    
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-s", "--station"):
            station_name = arg
            if not ((station_name == "all") or (station_name in STATION_NAMES_FOR_RJ)):
                print(arg_help)  # print the help message
                sys.exit(2)
        elif opt in ("-b", "--begin"):
            default_initial_year = arg
        elif opt in ("-e", "--end"):
            default_final_year = arg

    import_data(station_name, default_initial_year, default_final_year)


if __name__ == "__main__":
    main(sys.argv)


