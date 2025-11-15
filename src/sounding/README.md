# AtmoSeer – Radiosonde Retrieval & Atmospheric Instability Indices
  
Este conjunto de scripts permite:

✔️ Baixar radiossondas de **duas fontes oficiais**  
✔️ Padronizar os dados em formato Parquet  
✔️ Calcular índices de instabilidade com MetPy  
✔️ Incorporar metadados das estações  
✔️ Servir como base para previsão, análise e visualização

---

## Funcionalidades

### **1. Coleta de radiossondas – Wyoming Upper Air Archive (Siphon)**  
- Baixa sondagens atmosféricas dos horários **00Z e 12Z**  
- Usa a biblioteca **Siphon / WyomingUpperAir**  
- Retorna perfil vertical completo (pressão, vento, temperatura, etc.)

### **2. Cálculo de índices de instabilidade – MetPy**  
Gera índices clássicos de tempo severo:

- CAPE  
- CIN  
- Lifted Index  
- K Index  
- Total Totals  
- Showalter Index  

### **3. Coleta de radiossondas – NOAA IGRA v2 (igra library)**  
- Baixa arquivo completo da estação em formato ZIP  
- Converte para DataFrame  
- Incorpora **metadados da estação**:
  - latitude  
  - longitude  
  - altitude  
  - número de níveis  
  - tipo de sensor  
---

# 📁 Estrutura do Projeto

1️⃣ Instale as dependências

pip install pandas siphon metpy igra pyarrow requests


🎈 1. Baixar dados radiossondas – Wyoming (Siphon)
Script:
src/sounding/retrieve_uow.py

Usando datas:
python -m src.sounding.retrieve_uow -s SBGL --start_date 2021-01-01 --end_date 2021-01-31

Usando anos inteiros:
python -m src.sounding.retrieve_uow -s SBGL -b 2020 -e 2023

Saída:
data/as/SBGL_2021-01-01_2021-01-31.parquet.gzip

Visualizar tabela:
df = pd.read_parquet("data/as/SBGL_2021-01-01_2021-01-31.parquet.gzip", engine="pyarrow")
print(df.head())

🌩️ 2. Gerar índices de instabilidade – MetPy
Script:
src/sounding/gen_indices.py
Exemplo:
python -m src.sounding.gen_indices \
  --input_file data/as/SBGL_2021-01-01_2021-01-31.parquet.gzip \
  --output_file data/as/SBGL_indices.parquet.gzip

Visualizar tabela:
df = pd.read_parquet("data/as/SBGL_2021-01-01_2021-01-31_indices.parquet.gzip")
print(df.head())

🎈 3. Baixar radiossondas – IGRA v2 (NOAA)
Script:
src/sounding/retrieve_igra.py

Exemplo:
python -m src.sounding.retrieve_igra -s BRM00083746 --start_date 2018-01-01 --end_date 2020-12-31

Saída:
data/as/BRM00083746_2018-01-01_2020-12-31_igra.parquet.gzip

Visualizar tabela:
df_indices = pd.read_parquet("data/as/BRM00083746_2018-01-01_2018-01-31_igra.parquet.gzip")
print(df_indices.head())

