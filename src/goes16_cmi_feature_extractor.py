import os
import netCDF4 as nc
import numpy as np
import os
import netCDF4 as nc
from datetime import datetime, timedelta
import time

def glaciacao_topo_nuvem(pasta_entrada_canal11, 
                         pasta_entrada_canal14, 
                         pasta_entrada_canal15, 
                         pasta_saida):
    
    # Obter os anos disponíveis nas pastas dos canais
    anos_disponiveis = sorted(os.listdir(pasta_entrada_canal11))
    
    for ano in anos_disponiveis:
        caminho_ano_canal1 = os.path.join(pasta_entrada_canal11, ano)
        caminho_ano_canal2 = os.path.join(pasta_entrada_canal14, ano)
        caminho_ano_canal3 = os.path.join(pasta_entrada_canal15, ano)
        caminho_saida_ano = os.path.join(pasta_saida, ano)
        
        # Garantir que o diretório de saída para o ano existe
        os.makedirs(caminho_saida_ano, exist_ok=True)
        
        # Obter a lista de timestamps e prefixos dos canais
        arquivos_canal1 = sorted(os.listdir(caminho_ano_canal1))
        arquivos_canal2 = sorted(os.listdir(caminho_ano_canal2))
        arquivos_canal3 = sorted(os.listdir(caminho_ano_canal3))

        # Extrair os prefixos dos arquivos para os três canais
        prefix_canal1  = arquivos_canal1[0].split("_", 1)[0]
        prefix_canal2 = arquivos_canal2[0].split("_", 1)[0]
        prefix_canal3 = arquivos_canal3[0].split("_", 1)[0]

        # Prefixo da feature de glaciação do topo da nuvem
        prefix_feature = 'GTN'  

        # Obter os timestamps (considerando que os nomes dos arquivos seguem o padrão canal_timestamp)
        arquivos_canal_timestamp = [arquivo.split("_", 1)[1] for arquivo in arquivos_canal1 if "_" in arquivo]

        for timestamp in arquivos_canal_timestamp:
            
            # Gerar os nomes dos arquivos para os três canais
            arquivo_canal1 = f'{prefix_canal1}_{timestamp}'
            arquivo_canal2 = f'{prefix_canal2}_{timestamp}'
            arquivo_canal3 = f'{prefix_canal3}_{timestamp}'
            arquivo_saida = f'{prefix_feature}_{timestamp}'
            
            # Caminho completo para os arquivos de entrada e saída
            caminho_arquivo_canal1 = os.path.join(caminho_ano_canal1, arquivo_canal1)
            caminho_arquivo_canal2 = os.path.join(caminho_ano_canal2, arquivo_canal2)
            caminho_arquivo_canal3 = os.path.join(caminho_ano_canal3, arquivo_canal3)
            caminho_arquivo_saida = os.path.join(caminho_saida_ano, arquivo_saida)

            print(f'Calculando a diferença tri-espectral entre {arquivo_canal1}, {arquivo_canal2} e {arquivo_canal3}')
            
            # Verificar se a feature já foi computada
            if not os.path.exists(caminho_arquivo_saida):
                # Verificar se todos os arquivos correspondentes existem nos três canais
                if os.path.exists(caminho_arquivo_canal2) and os.path.exists(caminho_arquivo_canal3):
                    # Abrir os três arquivos e calcular a diferença tri-espectral
                    with nc.Dataset(caminho_arquivo_canal1, 'r') as canal1, \
                        nc.Dataset(caminho_arquivo_canal2, 'r') as canal2, \
                        nc.Dataset(caminho_arquivo_canal3, 'r') as canal3, \
                        nc.Dataset(caminho_arquivo_saida, 'w', format="NETCDF4") as saida:
                        
                        # Copiar as dimensões do primeiro canal para o arquivo de saída
                        for nome_dimensao, dimensao in canal1.dimensions.items():
                            saida.createDimension(nome_dimensao, len(dimensao) if not dimensao.isunlimited() else None)
                        
                        # Iterar sobre cada variável e calcular a diferença tri-espectral
                        for nome_variavel in canal1.variables:
                            if nome_variavel in canal2.variables and nome_variavel in canal3.variables:
                                dados_canal1 = canal1.variables[nome_variavel][:]
                                dados_canal2 = canal2.variables[nome_variavel][:]
                                dados_canal3 = canal3.variables[nome_variavel][:]
                                
                                # Calcular a diferença tri-espectral (diferença entre os três canais)
                                diferenca_tri_espectral = (dados_canal1 - dados_canal2) - (dados_canal2 - dados_canal3)

                                # Criar a nova variável no arquivo de saída com a diferença tri-espectral
                                variavel_saida = saida.createVariable(
                                    nome_variavel, 'f4', canal1.variables[nome_variavel].dimensions)
                                variavel_saida[:] = diferenca_tri_espectral
                                # Adicionar atributos, por exemplo, descrição da feature
                                variavel_saida.description = f"Diferença tri-espectral entre {prefix_canal1}, {prefix_canal2} e {prefix_canal3} para {nome_variavel}"
                            else:
                                print(f"Timestamp {nome_variavel} não encontrado nos três canais para {timestamp} em {ano}.")
                        
                        # Adicionar atributos globais ao arquivo de saída, se necessário
                        saida.description = "Arquivo com a feature baseada na diferença tri-espectral entre três canais"
                        saida.history = "Criado automaticamente"
                        saida.source = "Satélite GOES16"
                else:
                    print(f"Arquivo correspondente não encontrado para {timestamp} nos três canais em {ano}.")
            else:
                print(f"Arquivo {caminho_arquivo_saida} já foi computado.")

def profundidade_nuvens(pasta_entrada_canal9, pasta_entrada_canal13, pasta_saida):

    # Obter os anos disponíveis nas pastas dos canais
    anos_disponiveis = sorted(os.listdir(pasta_entrada_canal9))
    
    for ano in anos_disponiveis:
        caminho_ano_canal1 = os.path.join(pasta_entrada_canal9, ano)
        caminho_ano_canal2 = os.path.join(pasta_entrada_canal13, ano)
        caminho_saida_ano = os.path.join(pasta_saida, ano)
        
        # Garantir que o diretório de saída para o ano existe
        os.makedirs(caminho_saida_ano, exist_ok=True)
        
        # Obter a lista de timestamp e os prefixos dos canais
        arquivos_canal1 = sorted(os.listdir(caminho_ano_canal1))
        arquivos_canal2 = sorted(os.listdir(caminho_ano_canal2))

        prefix_canal1  = arquivos_canal1[0].split("_", 1)[0]
        prefix_canal2 = arquivos_canal2[0].split("_", 1)[0]

        # Prefixo da feature de profundidade das nuvens
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

            # Verificar se feature já não foi computada
            if not os.path.exists(caminho_arquivo_saida):
                # Verificar se o arquivo correspondente existe no outro canal
                if os.path.exists(caminho_arquivo_canal2):
                    # Abrir os arquivos e calcular a diferença
                    with nc.Dataset(caminho_arquivo_canal1, 'r') as canal1, \
                        nc.Dataset(caminho_arquivo_canal2, 'r') as canal2, \
                        nc.Dataset(caminho_arquivo_saida, 'w', format="NETCDF4") as saida:
                        
                        # Copiar as dimensões do primeiro canal para o arquivo de saída
                        for nome_dimensao, dimensao in canal1.dimensions.items():
                            saida.createDimension(nome_dimensao, len(dimensao) if not dimensao.isunlimited() else None)
                        
                        # Iterar sobre cada timestamp presente no canal1 e verificar no canal2
                        for nome_variavel in canal1.variables:
                            if nome_variavel in canal2.variables:
                                dados_canal1 = canal1.variables[nome_variavel][:]
                                dados_canal2 = canal2.variables[nome_variavel][:]
                                
                                # Calcular a diferença entre os dois canais
                                diferenca = dados_canal1 - dados_canal2

                                # Criar a nova variável no arquivo de saída com a diferença
                                variavel_saida = saida.createVariable(
                                    nome_variavel, 'f4', canal1.variables[nome_variavel].dimensions)
                                variavel_saida[:] = diferenca
                                # Adicionar atributos, por exemplo, descrição da feature
                                variavel_saida.description = "Diferença entre canal1 e canal2 para " + nome_variavel
                            else:
                                print(f"Timestamp {nome_variavel} não encontrado para {pasta_entrada_canal13} em {ano}.")
                        
                        # Adicionar atributos globais ao arquivo de saída, se necessário
                        saida.description = "Arquivo com a feature baseada na diferença entre dois canais"
                        saida.history = "Criado automaticamente"
                        saida.source = "Satélite GOES16"
                else:
                    print(f"Arquivo correspondente não encontrado para {arquivo_canal1} no canal2 e no ano {ano}.")
            else:
                print(f"Arquivo {caminho_arquivo_saida} já foi computado")

def derivada_temporal_fluxo_ascendente(pasta_entrada_canal, pasta_saida, intervalo_temporal=10):
    """
    Função para calcular a derivada temporal do fluxo ascendente (30 minutos),
    considerando dados com resolução temporal de 10 minutos (144 variáveis por arquivo).
    
    Args:
    - pasta_entrada_canal: Caminho para a pasta com arquivos de dados do canal.
    - pasta_saida: Caminho para a pasta de saída onde os resultados serão armazenados.
    - intervalo_temporal: Intervalo de tempo em arquivos (3 variáveis = 30 minutos) para calcular a derivada temporal.
    """
    
    # Obter os anos disponíveis nas pastas do canal
    anos_disponiveis = sorted(os.listdir(pasta_entrada_canal))
    
    for ano in anos_disponiveis:
        caminho_ano_canal = os.path.join(pasta_entrada_canal, ano)
        caminho_saida_ano = os.path.join(pasta_saida, ano)
        
        # Garantir que o diretório de saída para o ano exista
        os.makedirs(caminho_saida_ano, exist_ok=True)
        
        # Obter a lista de arquivos (cada arquivo contém dados para 24 horas)
        arquivos_canal = sorted(os.listdir(caminho_ano_canal))
        prefix_canal  = arquivos_canal[0].split("_", 1)[0]

        # Prefixo da feature de fluxo ascendente
        prefix_feature = 'FA'
        arquivos_canal_timestamp = [arquivo.split("_", 1)[1] for arquivo in arquivos_canal if "_" in arquivo]

        for arquivo in arquivos_canal_timestamp:
            caminho_arquivo_canal = os.path.join(caminho_ano_canal, f'{prefix_canal}_{arquivo}')
            caminho_arquivo_saida = os.path.join(caminho_saida_ano, f'{prefix_feature}_{arquivo}')
            
            print(f'Calculando a derivada temporal do fluxo ascendente no arquivo {arquivo}')
            
            # Verificar se a feature já foi computada
            if not os.path.exists(caminho_arquivo_saida):
                # Abrir o arquivo NetCDF
                with nc.Dataset(caminho_arquivo_canal, 'r') as canal, \
                     nc.Dataset(caminho_arquivo_saida, 'w', format="NETCDF4") as saida:
                    
                    # Copiar as dimensões do arquivo original para o arquivo de saída
                    for nome_dimensao, dimensao in canal.dimensions.items():
                        saida.createDimension(nome_dimensao, len(dimensao) if not dimensao.isunlimited() else None)
                    
                    # Iterar sobre as variáveis de dados no arquivo (cada variável corresponde a um timestamp)
                    for nome_variavel in canal.variables:

                        # Dividimos a string e removemos o prefixo 'CMI'
                        partes = nome_variavel.split('_')[1:]  
                        # Convertendo todas as partes para inteiros
                        year, mes, dia, hora, minuto = map(int, partes)  
                        # Avançando no timestamp
                        hora_adiante = datetime(year, mes, dia, hora, minuto) + timedelta(minutes=intervalo_temporal)
                        variavel_adiante = f"CMI_{hora_adiante.year:04}_{hora_adiante.month:02}_{hora_adiante.day:02}_{hora_adiante.hour:02}_{hora_adiante.minute:02}"

                        # Verificando se arquivo com avanço no timestamp existe
                        if variavel_adiante in canal.variables.keys():
                            # Calcular derivada temporal
                            delta_t = timedelta(minutes = intervalo_temporal).total_seconds() / 60
                            # print(f'delta_t: {delta_t}')
                            derivada_temporal = (canal.variables[variavel_adiante][:] - canal.variables[nome_variavel][:]) / delta_t

                            # Salvar no arquivo de saída
                            variavel_saida = saida.createVariable(nome_variavel, 'f4', canal.variables[nome_variavel].dimensions)
                            variavel_saida[:] = derivada_temporal
                            variavel_saida.description = f'Derivada temporal (fluxo ascendente) para {nome_variavel}'
                        else:
                            print(f'Não há instante de tempo {intervalo_temporal} minutos a frente para {nome_variavel}')
                    
                    # Adicionar atributos globais ao arquivo de saída, se necessário
                    saida.description = "Arquivo com a feature baseada na derivada temporal do fluxo ascendente"
                    saida.history = "Criado automaticamente"
                    saida.source = "Satélite GOES16"
            
            else:
                print(f"Arquivo {caminho_arquivo_saida} já foi computado.")

########################################################################
### MAIN
########################################################################

if __name__ == "__main__":
    start_time = time.time()  # Record the start time

    '''
    1- caminho da pasta do canal 9
    2- caminho da pasta do canal 13
    3- caminho da pasta de saída da feature
    '''
    profundidade_nuvens('./data/goes16/CMI/C09', 
                        './data/goes16/CMI/C13', 
                        './features/CMI/profundidade_nuvens') 

    '''
    1- caminho da pasta do canal 11
    2- caminho da pasta do canal 14
    3- caminho da pasta do canal 15
    4- caminho da pasta de saída da feature
    '''
    glaciacao_topo_nuvem('./data/goes16/CMI/C11', 
                         './data/goes16/CMI/C14', 
                         './data/goes16/CMI/C15', 
                         './features/CMI/glaciacao_topo_nuvem')

    ''' Feature dF_dt
    1- caminho da pasta do canal 13
    2- caminho da pasta de saída da feature
    '''
    derivada_temporal_fluxo_ascendente('./data/goes16/CMI/C13', 
                                       './features/CMI/dF_dt')

    end_time = time.time()  # Record the end time
    duration = (end_time - start_time) / 60  # Calculate duration in minutes
    print(f"Script execution time: {duration:.2f} minutes.")
