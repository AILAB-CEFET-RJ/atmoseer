import requests
from retrieve_data_cemaden import get_token
import VAR
import folium
url = "https://sws.cemaden.gov.br/PED/rest/pcds-cadastro/dados-cadastrais"


token = get_token(VAR.nome_secreto, VAR.senha_secreta)
codibge = "3304557"

headers = {
    "token": token
}

params = {
    "codibge": codibge,
    "formato": "json"  
}

response = requests.get(url, headers=headers, params=params)
posicoes = {}
if response.status_code == 200:
    estacoes = response.json()
    print("Número de estações encontradas:", len(estacoes))
    for estacao in estacoes:  
        codigo = estacao.get("codestacao")
        lat = estacao.get("latitude")
        lon = estacao.get("longitude")
        if codigo and lat and lon:
            posicoes[codigo] = (lat, lon)

    print("Dicionário de posições das estações:")
    for cod, coords in list(posicoes.items()): 
        print(f"{cod}: {coords}")
else:
    print("Erro ao buscar estações:", response.status_code)

mapa = folium.Map(location=[-22.9068, -43.1729], zoom_start=10)

for codestacao, (lat, lon) in posicoes.items():
    folium.Marker(
        location=[lat, lon],
        popup=f"Estação: {codestacao}",
        icon=folium.Icon(color="blue", icon="info-sign")
    ).add_to(mapa)

mapa.save("mapa_estacoes_rj.html")
print("Mapa gerado: mapa_estacoes_rj.html")