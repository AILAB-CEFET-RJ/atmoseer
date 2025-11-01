import datetime
import getopt
import os
import re
import sys

import numpy as np
import pandas as pd

STATION_NAMES_FOR_RJ = (
    "alto_da_boa_vista",
    "guaratiba",
    "iraja",
    "jardim_botanico",
    "riocentro",
    "santa_cruz",
    "sao_cristovao",
    "vidigal",
    "urca",
    "rocinha",
    "tijuca",
    "santa_teresa",
    "copacabana",
    "grajau",
    "ilha_do_governador",
    "penha",
    "madureira",
    "bangu",
    "piedade",
    "tanque",
    "saude",
    "barrinha",
    "cidade_de_deus",
    "grajau",
    "grande_meier",
    "anchieta",
    "grota_funda",
    "campo_grande",
    "sepetiba",
    "av_brasil_mendanha",
    "recreio",
    "laranjeiras",
    "tijuca_muda",
)


def corrige_txt(station_name, years, months):
    for year in years:
        for month in months:
            file = (
                "./data/RAW_data/COR/pluviometrica/"
                + station_name
                + "_"
                + year
                + month
                + "_Plv.txt"
            )
            if os.path.exists(file):
                fin = open(file, "rt")
                if not os.path.exists(
                    "./data/RAW_data/COR/pluviometrica/aux_" + station_name
                ):
                    os.mkdir("./data/RAW_data/COR/pluviometrica/aux_" + station_name)
                fout = open(
                    "./data/RAW_data/COR/pluviometrica/aux_"
                    + station_name
                    + "/"
                    + station_name
                    + "_"
                    + year
                    + month
                    + "_Plv2.txt",
                    "wt",
                )
                count = 0
                for line in fin:
                    count += 1
                    # fout.write(re.sub('\s+', ' ', line.replace(':00      ', ':00   HBV')))
                    text_to_write = re.sub(
                        "\s+",
                        " ",
                        line.replace("HBV", "")
                        .replace("05 min", "05_min")
                        .replace("10 min", "10_min")
                        .replace("15 min", "15_min")
                        .replace("01 h", "01_h")
                        .replace("04 h", "04_h")
                        .replace("24 h", "24_h")
                        .replace("96 h", "96_h"),
                    )
                    while len(text_to_write) < 34:
                        text_to_write = text_to_write + "ND "
                    fout.write(text_to_write)
                    fout.write("\n")
                if count == 5:
                    fout.write("01/" + month + "/" + year + " 00:00:00 ND ND ND ND ND")

                fin.close()
                fout.close()
            else:
                pass


def import_data(station_name, years, months, arg_end):
    df = pd.DataFrame()

    for year in years:
        check = 0
        for month in months:
            texto = (
                "./data/RAW_data/COR/pluviometrica/aux_"
                + station_name
                + "/"
                + station_name
                + "_"
                + year
                + month
                + "_Plv2.txt"
            )
            if os.path.exists(texto):
                df = pd.read_csv(texto, sep=" ", skiprows=[0, 1, 2, 3, 4], header=None)
                if len(df.columns) == 9 or len(df.columns) == 10:
                    if len(df.columns) == 10:
                        df.columns = [
                            "Dia",
                            "Hora",
                            "05 min",
                            "10 min",
                            "15 min",
                            "01 h",
                            "04 h",
                            "24 h",
                            "96 h",
                            "teste",
                        ]
                        del df["05 min"]
                        del df["10 min"]
                        del df["teste"]
                    else:
                        df.columns = [
                            "Dia",
                            "Hora",
                            "05 min",
                            "10 min",
                            "15 min",
                            "01 h",
                            "04 h",
                            "24 h",
                            "96 h",
                        ]
                        del df["05 min"]
                        del df["10 min"]
                else:
                    if len(df.columns) == 8:
                        df.columns = [
                            "Dia",
                            "Hora",
                            "15 min",
                            "01 h",
                            "04 h",
                            "24 h",
                            "96 h",
                            "teste",
                        ]
                        del df["teste"]
                    else:
                        df.columns = [
                            "Dia",
                            "Hora",
                            "15 min",
                            "01 h",
                            "04 h",
                            "24 h",
                            "96 h",
                        ]
                df["15 min"] = df["15 min"][~df["15 min"].isin(["-", "ND"])].astype(
                    float
                )
                df["01 h"] = df["01 h"][~df["01 h"].isin(["-", "ND"])].astype(float)
                df["04 h"] = df["04 h"][~df["04 h"].isin(["-", "ND"])].astype(float)
                df["24 h"] = df["24 h"][~df["24 h"].isin(["-", "ND"])].astype(float)
                df["96 h"] = df["96 h"][~df["96 h"].isin(["-", "ND"])].astype(float)
                df["Dia"] = pd.to_datetime(df["Dia"], format="%d/%m/%Y")

                ano_aux = year
                mes_aux = month
                print(year + "/" + month)
                check = 1
                break
            else:
                pass
        if check == 1:
            break

    ano1 = list(map(str, range(int(ano_aux), arg_end)))
    mes1 = list(range(int(mes_aux), 13))
    mes1 = [str(i).rjust(2, "0") for i in mes1]

    for year in ano1:
        for month in mes1:
            texto = (
                "./data/RAW_data/COR/pluviometrica/aux_"
                + station_name
                + "/"
                + station_name
                + "_"
                + year
                + month
                + "_Plv2.txt"
            )
            if os.path.exists(texto):
                print(texto)
                data2 = pd.read_csv(
                    texto,
                    sep=" ",
                    skiprows=[0, 1, 2, 3, 4],
                    header=None,
                    on_bad_lines="skip",
                )
                if len(data2.columns) == 9 or len(data2.columns) == 10:
                    if len(data2.columns) == 10:
                        data2.columns = [
                            "Dia",
                            "Hora",
                            "05 min",
                            "10 min",
                            "15 min",
                            "01 h",
                            "04 h",
                            "24 h",
                            "96 h",
                            "teste",
                        ]
                        del data2["05 min"]
                        del data2["10 min"]
                        del data2["teste"]
                    else:
                        data2.columns = [
                            "Dia",
                            "Hora",
                            "05 min",
                            "10 min",
                            "15 min",
                            "01 h",
                            "04 h",
                            "24 h",
                            "96 h",
                        ]
                        del data2["05 min"]
                        del data2["10 min"]
                else:
                    if len(data2.columns) == 8:
                        data2.columns = [
                            "Dia",
                            "Hora",
                            "15 min",
                            "01 h",
                            "04 h",
                            "24 h",
                            "96 h",
                            "teste",
                        ]
                        del data2["teste"]
                    else:
                        data2.columns = [
                            "Dia",
                            "Hora",
                            "15 min",
                            "01 h",
                            "04 h",
                            "24 h",
                            "96 h",
                        ]
                data2["15 min"] = data2["15 min"][
                    ~data2["15 min"].isin(["-", "ND"])
                ].astype(float)
                data2["01 h"] = data2["01 h"][~data2["01 h"].isin(["-", "ND"])].astype(
                    float
                )
                data2["04 h"] = data2["04 h"][~data2["04 h"].isin(["-", "ND"])].astype(
                    float
                )
                data2["24 h"] = data2["24 h"][~data2["24 h"].isin(["-", "ND"])].astype(
                    float
                )
                data2["96 h"] = data2["96 h"][~data2["96 h"].isin(["-", "ND"])].astype(
                    float
                )
                data2["Dia"] = pd.to_datetime(data2["Dia"], format="%d/%m/%Y")
                saida = pd.concat([df, data2])
                df = saida
                del saida
            else:
                pass
    if year == ano_aux:
        mes1 = list(range(1, 13))
        mes1 = [str(i).rjust(2, "0") for i in mes1]
    df = df.replace("ND", np.NaN)
    df = df.replace("-", np.NaN)
    print(df)
    # df = df[df['Hora'].str[2:6] == ':00:']

    # df['Hora'] = np.where(df['HBV'] == 'HBV', df['Hora'].str[0:2].astype(int) - 1, df['Hora'].str[0:2].astype(int))
    # df['Hora'] = np.where(df['Hora'] == -1, 23, df['Hora'])
    # df['Hora'] = df['Hora'].astype(str).str.zfill(2) + ':00:00'

    df["estacao"] = station_name
    if not os.path.exists("./data/landing/plv"):
        os.mkdir("./data/landing/plv")
    df.to_csv("./data/landing/plv/" + station_name + ".csv")
    data_aux = df
    del df, data2
    return data_aux


def import_data_cor(station_name, initial_year, final_year):
    years = list(map(str, range(int(initial_year), int(final_year))))
    months = list(range(1, 13))
    months = [str(i).rjust(2, "0") for i in months]

    if station_name == "all":
        station_names = STATION_NAMES_FOR_RJ
    else:
        station_names = [station_name]

    for station_name in station_names:
        print(station_name)
        corrige_txt(station_name, years, months)
        data = import_data(station_name, years, months, int(final_year))
        del data


def main(argv):
    station_name = ""
    default_initial_year = 1997

    today = datetime.date.today()
    default_final_year = today.year

    arg_help = "{0} -s <station> -b <begin> -e <end>".format(argv[0])

    try:
        opts, args = getopt.getopt(
            argv[1:], "hs:a:b:e:", ["help", "station=", "begin=", "end="]
        )
    except getopt.GetoptError as err:
        print(err)
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

    import_data_cor(station_name, default_initial_year, default_final_year)


if __name__ == "__main__":
    main(sys.argv)
