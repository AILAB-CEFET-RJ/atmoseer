#!/bin/bash

station_list=(
#"Ilha de Paquetá" 
#"Pilares" 
#"Higienópolis" 
#"Usina" 
#"Tijuca" 
#"Alto da Boa Vista" 
#"Vicente de Carvalho" 
#"Defesa Civil Santa Cruz" 
#"Vigário Geral" 
#"Pavuna" 
#"Catete" 
#"São Conrado" 
#"CIEP Dr. João Ramos de Souza" 
#"Jacarepaguá" 
#"Vargem Pequena" 
#"CIEP Samuel Wainer" 
#"Andaraí" 
#"Salgueiro" 
#"Padre Miguel" 
#"Realengo Batan" 
#"Santa Cruz" 
"Jardim Maravilha" 
"Est. Pedra Bonita" 
"Penha"  
"Tanque Jacarepagua"  
"Praça Seca"  
"Abolição"  
"Glória")

# Itera sobre o vetor
for station in "${station_list[@]}"; do
    echo "Processando arquivo: $station"
    
    python ./src/radar/sumare/correlation_new.py --start 2011-01-01 --end 2025-01-01 --system cemaden --station "${station}"

    echo "-----------------------------------"
done
