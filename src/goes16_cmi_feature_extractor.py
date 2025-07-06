import os
import netCDF4 as nc
import numpy as np
import os
import netCDF4 as nc
from datetime import datetime, timedelta
import time

def glaciacao_topo_nuvem(pasta_base, canal1_nome, canal2_nome, canal3_nome, pasta_saida):
    anos_disponiveis = sorted(os.listdir(pasta_base))
    for ano in anos_disponiveis:
        caminho_ano = os.path.join(pasta_base, ano)
        caminho_ano_canal1 = os.path.join(caminho_ano, canal1_nome)
        caminho_ano_canal2 = os.path.join(caminho_ano, canal2_nome)
        caminho_ano_canal3 = os.path.join(caminho_ano, canal3_nome)
        caminho_saida_ano = os.path.join(pasta_saida, ano)
        os.makedirs(caminho_saida_ano, exist_ok=True)
        arquivos_canal1 = sorted(os.listdir(caminho_ano_canal1))
        arquivos_canal2 = sorted(os.listdir(caminho_ano_canal2))
        arquivos_canal3 = sorted(os.listdir(caminho_ano_canal3))
        prefix_canal1  = arquivos_canal1[0].split("_", 1)[0]
        prefix_canal2 = arquivos_canal2[0].split("_", 1)[0]
        prefix_canal3 = arquivos_canal3[0].split("_", 1)[0]
        prefix_feature = 'GTN'
        arquivos_canal_timestamp = [arquivo.split("_", 1)[1] for arquivo in arquivos_canal1 if "_" in arquivo]
        for timestamp in arquivos_canal_timestamp:
            arquivo_canal1 = f'{prefix_canal1}_{timestamp}'
            arquivo_canal2 = f'{prefix_canal2}_{timestamp}'
            arquivo_canal3 = f'{prefix_canal3}_{timestamp}'
            arquivo_saida = f'{prefix_feature}_{timestamp}'
            caminho_arquivo_canal1 = os.path.join(caminho_ano_canal1, arquivo_canal1)
            caminho_arquivo_canal2 = os.path.join(caminho_ano_canal2, arquivo_canal2)
            caminho_arquivo_canal3 = os.path.join(caminho_ano_canal3, arquivo_canal3)
            caminho_arquivo_saida = os.path.join(caminho_saida_ano, arquivo_saida)
            print(f'Calculando a diferença tri-espectral entre {arquivo_canal1}, {arquivo_canal2} e {arquivo_canal3}')
            if not os.path.exists(caminho_arquivo_saida):
                if os.path.exists(caminho_arquivo_canal2) and os.path.exists(caminho_arquivo_canal3):
                    with nc.Dataset(caminho_arquivo_canal1, 'r') as canal1, \
                        nc.Dataset(caminho_arquivo_canal2, 'r') as canal2, \
                        nc.Dataset(caminho_arquivo_canal3, 'r') as canal3, \
                        nc.Dataset(caminho_arquivo_saida, 'w', format="NETCDF4") as saida:
                        for nome_dimensao, dimensao in canal1.dimensions.items():
                            saida.createDimension(nome_dimensao, len(dimensao) if not dimensao.isunlimited() else None)
                        for nome_variavel in canal1.variables:
                            if nome_variavel in canal2.variables and nome_variavel in canal3.variables:
                                dados_canal1 = canal1.variables[nome_variavel][:]
                                dados_canal2 = canal2.variables[nome_variavel][:]
                                dados_canal3 = canal3.variables[nome_variavel][:]
                                diferenca_tri_espectral = (dados_canal1 - dados_canal2) - (dados_canal2 - dados_canal3)
                                variavel_saida = saida.createVariable(
                                    nome_variavel, 'f4', canal1.variables[nome_variavel].dimensions)
                                variavel_saida[:] = diferenca_tri_espectral
                                variavel_saida.description = f"Diferença tri-espectral entre {prefix_canal1}, {prefix_canal2} e {prefix_canal3} para {nome_variavel}"
                            else:
                                print(f"Timestamp {nome_variavel} não encontrado nos três canais para {timestamp} em {ano}.")
                        saida.description = "Arquivo com a feature baseada na diferença tri-espectral entre três canais"
                        saida.history = "Criado automaticamente"
                        saida.source = "Satélite GOES16"
                else:
                    print(f"Arquivo correspondente não encontrado para {timestamp} nos três canais em {ano}.")
            else:
                print(f"Arquivo {caminho_arquivo_saida} já foi computado.")

def profundidade_nuvens(pasta_base, canal1_nome, canal2_nome, pasta_saida):
    anos_disponiveis = sorted(os.listdir(pasta_base))
    for ano in anos_disponiveis:
        caminho_ano = os.path.join(pasta_base, ano)
        caminho_ano_canal1 = os.path.join(caminho_ano, canal1_nome)
        caminho_ano_canal2 = os.path.join(caminho_ano, canal2_nome)
        caminho_saida_ano = os.path.join(pasta_saida, ano)
        os.makedirs(caminho_saida_ano, exist_ok=True)
        arquivos_canal1 = sorted(os.listdir(caminho_ano_canal1))
        arquivos_canal2 = sorted(os.listdir(caminho_ano_canal2))
        prefix_canal1  = arquivos_canal1[0].split("_", 1)[0]
        prefix_canal2 = arquivos_canal2[0].split("_", 1)[0]
        prefix_feature = 'PN'
        arquivos_canal_timestamp = [arquivo.split("_", 1)[1] for arquivo in arquivos_canal1 if "_" in arquivo]
        for timestamp in arquivos_canal_timestamp:
            arquivo_canal1 = f'{prefix_canal1}_{timestamp}'
            arquivo_canal2 = f'{prefix_canal2}_{timestamp}'
            arquivo_saida = f'{prefix_feature}_{timestamp}'
            caminho_arquivo_canal1 = os.path.join(caminho_ano_canal1, arquivo_canal1)
            caminho_arquivo_canal2 = os.path.join(caminho_ano_canal2, arquivo_canal2)
            caminho_arquivo_saida = os.path.join(caminho_saida_ano, arquivo_saida)
            print(f'Fazendo a diferença entre {arquivo_canal1} e {arquivo_canal2}')
            if not os.path.exists(caminho_arquivo_saida):
                if os.path.exists(caminho_arquivo_canal2):
                    with nc.Dataset(caminho_arquivo_canal1, 'r') as canal1, \
                        nc.Dataset(caminho_arquivo_canal2, 'r') as canal2, \
                        nc.Dataset(caminho_arquivo_saida, 'w', format="NETCDF4") as saida:
                        for nome_dimensao, dimensao in canal1.dimensions.items():
                            saida.createDimension(nome_dimensao, len(dimensao) if not dimensao.isunlimited() else None)
                        for nome_variavel in canal1.variables:
                            if nome_variavel in canal2.variables:
                                dados_canal1 = canal1.variables[nome_variavel][:]
                                dados_canal2 = canal2.variables[nome_variavel][:]
                                diferenca = dados_canal1 - dados_canal2
                                variavel_saida = saida.createVariable(
                                    nome_variavel, 'f4', canal1.variables[nome_variavel].dimensions)
                                variavel_saida[:] = diferenca
                                variavel_saida.description = "Diferença entre canal1 e canal2 para " + nome_variavel
                            else:
                                print(f"Timestamp {nome_variavel} não encontrado para {canal2_nome} em {ano}.")
                        saida.description = "Arquivo com a feature baseada na diferença entre dois canais"
                        saida.history = "Criado automaticamente"
                        saida.source = "Satélite GOES16"
                else:
                    print(f"Arquivo correspondente não encontrado para {arquivo_canal1} no canal2 e no ano {ano}.")
            else:
                print(f"Arquivo {caminho_arquivo_saida} já foi computado")

def derivada_temporal_fluxo_ascendente(pasta_base, canal_nome, pasta_saida, intervalo_temporal=10):
    anos_disponiveis = sorted(os.listdir(pasta_base))
    for ano in anos_disponiveis:
        caminho_ano = os.path.join(pasta_base, ano)
        caminho_ano_canal = os.path.join(caminho_ano, canal_nome)
        caminho_saida_ano = os.path.join(pasta_saida, ano)
        os.makedirs(caminho_saida_ano, exist_ok=True)
        arquivos_canal = sorted(os.listdir(caminho_ano_canal))
        prefix_canal  = arquivos_canal[0].split("_", 1)[0]
        prefix_feature = 'FA'
        arquivos_canal_timestamp = [arquivo.split("_", 1)[1] for arquivo in arquivos_canal if "_" in arquivo]
        for arquivo in arquivos_canal_timestamp:
            caminho_arquivo_canal = os.path.join(caminho_ano_canal, f'{prefix_canal}_{arquivo}')
            caminho_arquivo_saida = os.path.join(caminho_saida_ano, f'{prefix_feature}_{arquivo}')
            print(f'Calculando a derivada temporal do fluxo ascendente no arquivo {arquivo}')
            if not os.path.exists(caminho_arquivo_saida):
                with nc.Dataset(caminho_arquivo_canal, 'r') as canal, \
                     nc.Dataset(caminho_arquivo_saida, 'w', format="NETCDF4") as saida:
                    for nome_dimensao, dimensao in canal.dimensions.items():
                        saida.createDimension(nome_dimensao, len(dimensao) if not dimensao.isunlimited() else None)
                    for nome_variavel in canal.variables:
                        partes = nome_variavel.split('_')[1:]
                        year, mes, dia, hora, minuto = map(int, partes)
                        hora_adiante = datetime(year, mes, dia, hora, minuto) + timedelta(minutes=intervalo_temporal)
                        variavel_adiante = f"CMI_{hora_adiante.year:04}_{hora_adiante.month:02}_{hora_adiante.day:02}_{hora_adiante.hour:02}_{hora_adiante.minute:02}"
                        if variavel_adiante in canal.variables.keys():
                            delta_t = timedelta(minutes = intervalo_temporal).total_seconds() / 60
                            derivada_temporal = (canal.variables[variavel_adiante][:] - canal.variables[nome_variavel][:]) / delta_t
                            variavel_saida = saida.createVariable(nome_variavel, 'f4', canal.variables[nome_variavel].dimensions)
                            variavel_saida[:] = derivada_temporal
                            variavel_saida.description = f'Derivada temporal (fluxo ascendente) para {nome_variavel}'
                        else:
                            print(f'Não há instante de tempo {intervalo_temporal} minutos a frente para {nome_variavel}')
                    saida.description = "Arquivo com a feature baseada na derivada temporal do fluxo ascendente"
                    saida.history = "Criado automaticamente"
                    saida.source = "Satélite GOES16"
            else:
                print(f"Arquivo {caminho_arquivo_saida} já foi computado.")

########################################################################
### MAIN
########################################################################

if __name__ == "__main__":
    start_time = time.time()
    profundidade_nuvens('./data/goes16/CMI', 'C09', 'C13', './features/CMI/profundidade_nuvens')
    glaciacao_topo_nuvem('./data/goes16/CMI', 'C11', 'C14', 'C15', './features/CMI/glaciacao_topo_nuvem')
    derivada_temporal_fluxo_ascendente('./data/goes16/CMI', 'C13', './features/CMI/dF_dt')
    end_time = time.time()
    duration = (end_time - start_time) / 60
    print(f"Script execution time: {duration:.2f} minutes.")
