# GOES-16 Feature Extraction (corrigido)
# Este módulo contém funções para extrair várias features dos dados do satélite GOES-16.

import os
import netCDF4 as nc
import numpy as np
from datetime import datetime, timedelta
from scipy.ndimage import generic_filter

# PN: Profundidade da Nuvem
def profundidade_nuvens(pasta_entrada_canal9, pasta_entrada_canal13, pasta_saida):
    arquivos_canal9 = sorted(os.listdir(pasta_entrada_canal9))
    arquivos_canal13 = sorted(os.listdir(pasta_entrada_canal13))
    prefix_canal9 = arquivos_canal9[0].split("_", 1)[0]
    prefix_canal13 = arquivos_canal13[0].split("_", 1)[0]
    prefix_feature = 'PN'
    arquivos_timestamp = [f.split("_", 1)[1] for f in arquivos_canal9 if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for timestamp in arquivos_timestamp:
        arq_entrada1 = os.path.join(pasta_entrada_canal9, f"{prefix_canal9}_{timestamp}")
        arq_entrada2 = os.path.join(pasta_entrada_canal13, f"{prefix_canal13}_{timestamp}")
        arq_saida = os.path.join(pasta_saida, f"{prefix_feature}_{timestamp}")
        if not os.path.exists(arq_saida) and os.path.exists(arq_entrada2):
            with nc.Dataset(arq_entrada1, 'r') as nc1, nc.Dataset(arq_entrada2, 'r') as nc2, nc.Dataset(arq_saida, 'w') as out:
                for nome_dim, dim in nc1.dimensions.items():
                    out.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                for nome_var in nc1.variables:
                    if nome_var in nc2.variables:
                        dados = nc1.variables[nome_var][:] - nc2.variables[nome_var][:]
                        var_out = out.createVariable(nome_var, 'f4', nc1.variables[nome_var].dimensions)
                        var_out[:] = dados
                        var_out.description = "PN: profundidade da nuvem (C09 - C13)"
                out.description = "Diferença entre canais para profundidade da nuvem"

# GTN: Glaciação do Topo da Nuvem
def glaciacao_topo_nuvem(pasta_entrada_canal11, pasta_entrada_canal14, pasta_entrada_canal15, pasta_saida):
    arquivos_c11 = sorted(os.listdir(pasta_entrada_canal11))
    arquivos_c14 = sorted(os.listdir(pasta_entrada_canal14))
    arquivos_c15 = sorted(os.listdir(pasta_entrada_canal15))
    pfx11 = arquivos_c11[0].split("_", 1)[0]
    pfx14 = arquivos_c14[0].split("_", 1)[0]
    pfx15 = arquivos_c15[0].split("_", 1)[0]
    pfx_feat = 'GTN'
    timestamps = [f.split("_", 1)[1] for f in arquivos_c11 if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for timestamp in timestamps:
        arq11 = os.path.join(pasta_entrada_canal11, f"{pfx11}_{timestamp}")
        arq14 = os.path.join(pasta_entrada_canal14, f"{pfx14}_{timestamp}")
        arq15 = os.path.join(pasta_entrada_canal15, f"{pfx15}_{timestamp}")
        saida = os.path.join(pasta_saida, f"{pfx_feat}_{timestamp}")
        if not os.path.exists(saida) and os.path.exists(arq14) and os.path.exists(arq15):
            with nc.Dataset(arq11, 'r') as nc1, nc.Dataset(arq14, 'r') as nc2, nc.Dataset(arq15, 'r') as nc3, nc.Dataset(saida, 'w') as out:
                for nome_dim, dim in nc1.dimensions.items():
                    out.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                for nome_var in nc1.variables:
                    if nome_var in nc2.variables and nome_var in nc3.variables:
                        dados = (nc1.variables[nome_var][:] - nc2.variables[nome_var][:]) - (nc2.variables[nome_var][:] - nc3.variables[nome_var][:])
                        var_out = out.createVariable(nome_var, 'f4', nc1.variables[nome_var].dimensions)
                        var_out[:] = dados
                        var_out.description = 'GTN: glaciação topo da nuvem (tri-espectral)'
                out.description = "Glaciação topo da nuvem"

# FA: Derivada Temporal do Fluxo Ascendente
def derivada_temporal_fluxo_ascendente(pasta_entrada_canal, pasta_saida, intervalo_temporal=10):
    arquivos = sorted(os.listdir(pasta_entrada_canal))
    pfx = arquivos[0].split("_", 1)[0]
    timestamps = [f.split("_", 1)[1] for f in arquivos if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for ts in timestamps:
        atual = os.path.join(pasta_entrada_canal, f"{pfx}_{ts}")
        saida = os.path.join(pasta_saida, f"FA_{ts}")
        if not os.path.exists(saida):
            with nc.Dataset(atual, 'r') as src, nc.Dataset(saida, 'w') as dst:
                for nome_dim, dim in src.dimensions.items():
                    dst.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                for nome_var in src.variables:
                    partes = nome_var.split('_')[1:]
                    try:
                        year, mes, dia, hora, minuto = map(int, partes)
                        hora_adiante = datetime(year, mes, dia, hora, minuto) + timedelta(minutes=intervalo_temporal)
                        nome_adiante = f"CMI_{hora_adiante.year:04}_{hora_adiante.month:02}_{hora_adiante.day:02}_{hora_adiante.hour:02}_{hora_adiante.minute:02}"
                        if nome_adiante in src.variables:
                            delta_t = intervalo_temporal
                            derivada = (src.variables[nome_adiante][:] - src.variables[nome_var][:]) / delta_t
                            var_out = dst.createVariable(nome_var, 'f4', src.variables[nome_var].dimensions)
                            var_out[:] = derivada
                            var_out.description = f"Derivada temporal de {nome_var}"
                    except ValueError:
                        continue
                dst.description = "Derivada temporal do fluxo ascendente"

# WV_grad: Gradiente de Vapor d'Água
def gradiente_vapor_agua(pasta_c09, pasta_c08, pasta_saida):
    arquivos_c09 = sorted(os.listdir(pasta_c09))
    arquivos_c08 = sorted(os.listdir(pasta_c08))
    prefix_c09 = arquivos_c09[0].split("_", 1)[0]
    prefix_c08 = arquivos_c08[0].split("_", 1)[0]
    prefix_feature = 'WV_grad'
    arquivos_timestamp = [f.split("_", 1)[1] for f in arquivos_c09 if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for timestamp in arquivos_timestamp:
        path_c09 = os.path.join(pasta_c09, f'{prefix_c09}_{timestamp}')
        path_c08 = os.path.join(pasta_c08, f'{prefix_c08}_{timestamp}')
        path_saida = os.path.join(pasta_saida, f'{prefix_feature}_{timestamp}')

        if not os.path.exists(path_saida) and os.path.exists(path_c08):
            with nc.Dataset(path_c09, 'r') as nc1, nc.Dataset(path_c08, 'r') as nc2, nc.Dataset(path_saida, 'w') as out:
                for nome_dim, dim in nc1.dimensions.items():
                    out.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                for nome_var in nc1.variables:
                    if nome_var in nc2.variables:
                        dados = nc1.variables[nome_var][:] - nc2.variables[nome_var][:]
                        var_out = out.createVariable(nome_var, 'f4', nc1.variables[nome_var].dimensions)
                        var_out[:] = dados
                        var_out.description = f'Diferença C09 - C08 (vapor d’água)'
                out.description = 'Gradiente espectral do vapor d’água'

# LI_proxy: Proxy de Estabilidade Atmosférica
def proxy_estabilidade(pasta_c14, pasta_c13, pasta_saida):
    arquivos_c14 = sorted(os.listdir(pasta_c14))
    arquivos_c13 = sorted(os.listdir(pasta_c13))
    prefix_c14 = arquivos_c14[0].split("_", 1)[0]
    prefix_c13 = arquivos_c13[0].split("_", 1)[0]
    prefix_feature = 'LI_proxy'
    arquivos_timestamp = [f.split("_", 1)[1] for f in arquivos_c14 if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for timestamp in arquivos_timestamp:
        path_c14 = os.path.join(pasta_c14, f'{prefix_c14}_{timestamp}')
        path_c13 = os.path.join(pasta_c13, f'{prefix_c13}_{timestamp}')
        path_saida = os.path.join(pasta_saida, f'{prefix_feature}_{timestamp}')

        if not os.path.exists(path_saida) and os.path.exists(path_c13):
            with nc.Dataset(path_c14, 'r') as nc1, nc.Dataset(path_c13, 'r') as nc2, nc.Dataset(path_saida, 'w') as out:
                for nome_dim, dim in nc1.dimensions.items():
                    out.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                for nome_var in nc1.variables:
                    if nome_var in nc2.variables:
                        dados = nc1.variables[nome_var][:] - nc2.variables[nome_var][:]
                        var_out = out.createVariable(nome_var, 'f4', nc1.variables[nome_var].dimensions)
                        var_out[:] = dados
                        var_out.description = f'C14 - C13 como proxy de estabilidade'
                out.description = 'Proxy de estabilidade atmosférica com canais C14 e C13'

# TOCT: Temperatura do Topo da Nuvem
def temperatura_topo_nuvem(pasta_c13, pasta_saida):
    arquivos = sorted(os.listdir(pasta_c13))
    prefix = arquivos[0].split("_", 1)[0]
    prefix_feature = 'TOCT'
    arquivos_timestamp = [f.split("_", 1)[1] for f in arquivos if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for timestamp in arquivos_timestamp:
        path_entrada = os.path.join(pasta_c13, f"{prefix}_{timestamp}")
        path_saida = os.path.join(pasta_saida, f"{prefix_feature}_{timestamp}")

        if not os.path.exists(path_saida):
            with nc.Dataset(path_entrada, 'r') as src, nc.Dataset(path_saida, 'w') as dst:
                for nome_dim, dim in src.dimensions.items():
                    dst.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                for nome_var in src.variables:
                    dados = src.variables[nome_var][:]
                    var_out = dst.createVariable(nome_var, 'f4', src.variables[nome_var].dimensions)
                    var_out[:] = dados
                    var_out.description = 'Temperatura do topo da nuvem (TOCT) diretamente do canal 13'
                dst.description = 'TOCT a partir do canal C13'

# PNstd: Textura Local da Profundidade da Nuvem
def textura_local_profundidade(pasta_pn, pasta_saida, tamanho_janela=3):
    arquivos = sorted(os.listdir(pasta_pn))
    prefix = arquivos[0].split("_", 1)[0]
    prefix_feature = 'PNstd'
    arquivos_timestamp = [f.split("_", 1)[1] for f in arquivos if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for timestamp in arquivos_timestamp:
        path_entrada = os.path.join(pasta_pn, f"{prefix}_{timestamp}")
        path_saida = os.path.join(pasta_saida, f"{prefix_feature}_{timestamp}")

        if not os.path.exists(path_saida):
            with nc.Dataset(path_entrada, 'r') as src, nc.Dataset(path_saida, 'w') as dst:
                for nome_dim, dim in src.dimensions.items():
                    dst.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                for nome_var in src.variables:
                    dados = src.variables[nome_var][:]
                    std_local = generic_filter(dados, np.std, size=tamanho_janela, mode='nearest')
                    var_out = dst.createVariable(nome_var, 'f4', src.variables[nome_var].dimensions)
                    var_out[:] = std_local
                    var_out.description = 'Desvio padrão espacial local (textura) da profundidade da nuvem'
                dst.description = 'Textura espacial (desvio padrão local) da feature PN'
