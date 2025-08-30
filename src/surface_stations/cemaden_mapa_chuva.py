import os
import pandas as pd
import matplotlib.pyplot as plt
import requests
from pathlib import Path
import folium
from retrieve_data_cemaden import get_token
import VAR

def get_estacao_posicoes(codibge="3304557"):
    url = "https://sws.cemaden.gov.br/PED/rest/pcds-cadastro/dados-cadastrais"
    token = get_token(VAR.nome_secreto, VAR.senha_secreta)
    headers = {"token": token}
    params = {"codibge": codibge, "formato": "json"}
    response = requests.get(url, headers=headers, params=params)
    
    posicoes = {}
    if response.status_code == 200:
        estacoes = response.json()
        for est in estacoes:
            cod = est.get("codestacao")
            lat = est.get("latitude")
            lon = est.get("longitude")
            if cod and lat and lon:
                posicoes[cod] = (lat, lon)
    return posicoes

def calcular_chuva_por_estacao(raw_folder="data/ws/cemaden/raw/"):
    chuva_total = {}
    for file in os.listdir(raw_folder):
        if file.endswith(".parquet"):
            cod = Path(file).stem
            caminho = os.path.join(raw_folder, file)
            try:
                df = pd.read_parquet(caminho)
                if "chuva" in df.columns and not df["chuva"].isna().all():
                    total = df["chuva"].sum(skipna=True)
                    chuva_total[cod] = total
            except Exception as e:
                print(f"Erro ao ler {file}: {e}")
    return chuva_total

def plotar_mapa_matplotlib(chuva_total, posicoes):
    lats, lons, intensidades = [], [], []
    for cod, total in chuva_total.items():
        if cod in posicoes:
            lat, lon = posicoes[cod]
            lats.append(lat)
            lons.append(lon)
            intensidades.append(total)

    if not lats:
        print("Nenhuma estação com dados e coordenadas disponíveis.")
        return

    plt.figure(figsize=(10, 8))
    scatter = plt.scatter(lons, lats, c=intensidades, cmap="viridis", s=100, edgecolor='k')
    plt.colorbar(scatter, label="Chuva acumulada (mm)")
    plt.title("Estações e precipitação acumulada")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def gerar_mapa_com_folium(chuva_total, posicoes, saida_html="mapa_chuva.html"):
    mapa = folium.Map(location=[-22.9, -43.2], zoom_start=8)

    if not chuva_total:
        print("Nenhuma estação com chuva registrada encontrada.")
        return

    max_chuva = max(chuva_total.values())

    for cod, total in chuva_total.items():
        if cod in posicoes:
            lat, lon = posicoes[cod]
            cor = "green" if total < 10 else "orange" if total < 50 else "red"
            raio = 2000 + (total / max_chuva) * 3000

            folium.Circle(
                location=[lat, lon],
                radius=raio,
                color=cor,
                fill=True,
                fill_color=cor,
                fill_opacity=0.6,
                popup=f"Estação: {cod}<br>Chuva: {total:.1f} mm"
            ).add_to(mapa)

    mapa.save(saida_html)
    print(f"Mapa interativo salvo em: {saida_html}")



posicoes = get_estacao_posicoes()
chuva_total = calcular_chuva_por_estacao()

plotar_mapa_matplotlib(chuva_total, posicoes)
gerar_mapa_com_folium(chuva_total, posicoes)
