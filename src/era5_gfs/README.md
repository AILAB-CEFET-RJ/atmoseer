# Download do ERA5 (Earth Data Hub + CDS) e do GFS

Baixa o ERA5 horário, nos níveis de pressão e em single level, para um recorte qualquer.
O padrão é o Rio de Janeiro: grade nativa 0,25° de 9 × 11 pontos, lat −21.75..−23.75,
lon −45.00..−42.50. Também baixa o GFS 0,25° no mesmo formato, para avaliar em "tempo
real" um modelo treinado com ERA5.

O trabalho é dividido em duas etapas independentes:

| etapa | script | o que faz |
|---|---|---|
| 1 | `baixar_era5.py` | cada **(variável, mês)** vira um arquivo em `era5-bruto/`. Uma falha numa variável não derruba as outras, e rodar de novo só baixa o que falta. |
| 2 | `juntar_era5.py` | junta por mês em `era5-{single,pressure}-levels-conv/RJ_<ano>_<mes>.nc` (coordenadas `valid_time`, `latitude`, `longitude`, `pressure_level`) |

**O que baixar** fica todo em `era5_catalogo.py`: domínio, período, níveis e uma linha
por variável. Para acrescentar ou tirar uma variável, basta editar uma linha.

## Fontes

* **EDH, o Earth Data Hub (DestinE).** É um espelho oficial do ERA5 em Zarr, sem fila, e
  baixa quase tudo em horas.
  * Os valores vêm arredondados a 10 bits de mantissa: erro relativo ≤ 2⁻¹¹ ≈ 0,05 %, sem viés.
  * `conferir_edh_x_cds.py` comprova isso contra um mês baixado do CDS.
* **CDS, o Copernicus Climate Data Store.** Só serve para o que o EDH não tem: `totalx` e
  `vimdf`.
  * Tem fila: cada pedido leva horas.
  * O script divide automaticamente um pedido grande demais (`cost limits exceeded`).
  * Pede `totalx` sozinho, porque misturado com outras variáveis ele vem vazio sem aviso.

## Instalação

O Zarr 3 exige Python ≥ 3.11.

```bash
conda create -n era5 python=3.12 -y
conda activate era5
pip install -r src/era5_gfs/requirements-era5.txt
```

## Credenciais (nunca versionar)

* **EDH:** crie uma conta em https://earthdatahub.destine.eu, gere uma chave de API e
  rode `export EDH_API_KEY=<sua chave>`. Ela pode ficar num `.env` local, que já está no
  `.gitignore`.
* **CDS:** crie uma conta em https://cds.climate.copernicus.eu, aceite as licenças do ERA5
  e crie o `~/.cdsapirc`:
  ```
  url: https://cds.climate.copernicus.eu/api
  key: <sua chave>
  ```

## Uso

Os dados são gravados no diretório atual. Use `--raiz <dir>` ou `ERA5_RAIZ=<dir>` para
mudar o destino.

```bash
python src/era5_gfs/baixar_era5.py --listar                 # catálogo agrupado por tipo físico
python src/era5_gfs/baixar_era5.py --dry-run                # o que seria baixado
python src/era5_gfs/baixar_era5.py --status                 # ok / falha / pendente, por variável e mês

# teste rápido: um mês, algumas variáveis
python src/era5_gfs/baixar_era5.py --anos 2020 --meses 1 --vars cape cin tcwv tp

# lote completo: EDH e CDS em processos separados (o CDS leva dias)
nohup python -u src/era5_gfs/baixar_era5.py --fonte edh > baixar_edh.log 2>&1 &
nohup python -u src/era5_gfs/baixar_era5.py --fonte cds > baixar_cds.log 2>&1 &

# etapa 2: junta por mês. Só grava mês completo; --parcial grava mesmo faltando variável
python src/era5_gfs/juntar_era5.py
```

* Cada tentativa fica registrada em `era5-bruto/_registro.jsonl`, com variável, mês, fonte,
  status e motivo.
* **Se o processo morrer**, por reinício ou queda de rede, rode o mesmo comando de novo.
  O que já está gravado é pulado, e os arquivos são gravados de forma atômica (`.part` → rename).

### Conferir EDH × CDS

```bash
python src/era5_gfs/baixar_era5.py --anos 2020 --meses 1 --vars cape cin tcwv vimdf sp tp
python src/era5_gfs/conferir_edh_x_cds.py     # usa era5-bruto/_gabarito_cds/ (arquivos do CDS)
```

## GFS (`baixar_gfs.py`)

O script baixa o GFS 0,25° de hora em hora pelo [Herbie](https://herbie.readthedocs.io),
do arquivo do AWS, que começa em fev/2021. A saída vai para
`gfs-bruto/<conjunto>/<var>/RJ_<ano>_<mes>.nc`, com a **mesma grade, os mesmos nomes
curtos e as mesmas unidades** do `era5-bruto/`.

**Como a série horária é montada.** Para cada hora usa a rodada mais recente (00, 06, 12
ou 18 UTC) com prazo de 1 a 6 h. Os acumulados e as médias são convertidos para valor
horário.

**Correspondência com o ERA5:**
* algumas variáveis são derivadas: `vo` vem da vorticidade absoluta menos f, e `kx` e
  `totalx` saem de T e Td;
* `d`, `pv`, `cc`, `tciw` e `vimdf` não existem no GFS (detalhes no cabeçalho do script).

```bash
python src/era5_gfs/baixar_gfs.py --listar
python src/era5_gfs/baixar_gfs.py --estimar                  # volume a transferir (lê só um .idx)
python src/era5_gfs/baixar_gfs.py --anos 2022 --meses 1      # um mês de teste
python src/era5_gfs/baixar_gfs.py --status
```

## Testes

Não usam rede:

```bash
python -m pytest src/era5_gfs/tests -q
```
