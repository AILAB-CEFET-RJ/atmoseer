import pandas as pd
import requests
import os
import json
import time
import io
import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta

from config import globals

TOKEN_FILE = "./config/token_demaden.json"
rota = "https://sws.cemaden.gov.br/PED/rest"
rota_token = "https://sgaa.cemaden.gov.br/SGAA/rest"

def get_token(email, senha):
    url = rota_token + "/controle-token/tokens"
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            token_data = json.load(f)
        saved_at = token_data.get("saved_at", 0)
        time_to_exp = int(token_data.get("timeToExp", 0))
        if int(time.time()) - saved_at <= time_to_exp:
            return token_data.get("token", 0)

    payload = {
        "email": email,
        "password": senha
    }
    response = requests.post(url, json=payload)
    token_data = response.json()
    token_data["saved_at"] = int(time.time())

    # with open(TOKEN_FILE, "w") as f:
    #     json.dump(token_data, f)

    return token_data.get("token", 0)


def get_estacoes(codibge, email, senha):
    url = rota + "/pcds-cadastro/estacoes"
    token = get_token(email, senha)
    headers = {"token": token}
    tipos_est = requests.get(rota + "/pcds-tipo-estacao/sensores", headers=headers).json()
    estacoes_tipo = {e['tipoestacao']: e for e in tipos_est}

    estacoes = []
    for cod in codibge:
        time.sleep(5)
        params = {"codibge": cod}
        response = requests.get(url, params=params, headers=headers).json()
        for est in response:
            cod_estacao = est.get("codestacao", 0)
            tipo_id = est.get("id_tipoestacao", None)
            sensores = [s["sensordescricao"] for s in estacoes_tipo.get(tipo_id, {}).get("sensor", [])]
            estacoes.append((cod_estacao, sensores))
    return estacoes


def get_dados_estacoes(codibge, email, senha, inicio=None, fim=None, codestacao=None):
    url = rota + "/pcds/dados_pcd"

    if codestacao:
        estacoes = [(codestacao, [])]  # Lista com uma estação só
    else:
        estacoes = get_estacoes(codibge, email, senha)

    print("estações ", estacoes)
    start_date = datetime.strptime(inicio, "%Y-%m-%d") if inicio else datetime(2015, 1, 1)
    end_date = datetime.strptime(fim, "%Y-%m-%d") if fim else datetime.today()

    for est in estacoes:
        token = get_token(email, senha)
        current = start_date
        todos_dados = []
        headers = {"token": token}

        while current < end_date:
            data_inicio = current.strftime("%Y%m010000")
            proximo_mes = current + relativedelta(months=1)
            data_fim = proximo_mes.strftime("%Y%m010000")

            params = {
                "inicio": data_inicio,
                "fim": data_fim,
                "rede": "11",
                "codigo": est[0]
            }

            print(f"Baixando: {data_inicio} até {data_fim} para estação {est[0]}")
            response = requests.get(url, params=params, headers=headers)

            if response.status_code == 200:
                try:
                    df = pd.read_csv(io.StringIO(response.text), sep=";", skiprows=1)
                    if df.empty:
                        continue
                    df_pivot = df.pivot(index='datahora', columns='sensor', values='valor').reset_index()
                    todos_dados.append(df_pivot)
                except Exception as e:
                    print(f"Erro ao processar: {e}")
            else:
                print(f"Erro HTTP {response.status_code}: {response.text}")
            current = proximo_mes
            time.sleep(5)

        if todos_dados:
            df_final = pd.concat(todos_dados, ignore_index=True)
            filename = f"{est[0]}.parquet"
            output_path = os.path.join(globals.CEMADEN_DATA_DIR, "raw", filename)
            df_final.to_parquet(output_path, index=False)
            print(f"Dados salvos em: {output_path}")
        else:
            print(f"Nenhum dado encontrado para estação {est[0]}.")


def main():
    parser = argparse.ArgumentParser(description="Baixa dados meteorológicos por código IBGE ou estação usando API do Cemaden.")
    parser.add_argument("--ibge", nargs="+", help="Código(s) IBGE do município (ex: 3304557)")
    parser.add_argument("--inicio", help="Data inicial no formato YYYY-MM-DD", default=None)
    parser.add_argument("--fim", help="Data final no formato YYYY-MM-DD", default=None)
    parser.add_argument("--email", required=True, help="E-mail de autenticação na API")
    parser.add_argument("--senha", required=True, help="Senha de autenticação na API")
    parser.add_argument("--estacao", required=False, help="Código de uma estação específica (ex: A627)")

    args = parser.parse_args()

    # Validação básica: se não passou estação, precisa passar pelo menos 1 código IBGE
    if not args.estacao and not args.ibge:
        parser.error("Você deve fornecer --ibge ou --estacao.")

    get_dados_estacoes(
        codibge=args.ibge,
        email=args.email,
        senha=args.senha,
        inicio=args.inicio,
        fim=args.fim,
        codestacao=args.estacao
    )


if __name__ == "__main__":
    main()
