##SCRIPT DI ESTRAZIONE DEI DATI SUI PREZZI ENERGETICI DAL SITO GME 
## E' POSSIBILE OTTENERE I DATI TRAMITE SERVER FTP PREVIA RICHIESTA AL GME, OVVERO TRAMITE DOWNLOAD MANUALE 
import pandas as pd
import os
from datetime import datetime
import numpy as np
from matplotlib import pyplot as plt
from configurazione.init import init_path
import configparser

#N.B. verificare la correttezza di tutti i percorsi
config = configparser.ConfigParser()
config.read(init_path)
origin_path = config['user']['origin_path_gme'] #percorso contenente la cartella "curve_gme"
data_path = config['user']['data_path_gme']


# SELEZIONE ANNO DI RIFERIMENTO E ZONA GEOGRAFICA
anno = int(input('Inserire anno in formato YYYY: '))
print('Hai selezionato: ', anno)
lista_geo_zone = ['SICI', 'NORD', 'SARD', 'CSUD', 'CNORD', 'SUD', 'PUN']
dict_lista_zone_geo = {'SICI':'sici', 'NORD':'nord', 'SARD':'sard', 'CSUD':'csud', 'CNORD':'cnord', 'SUD':'sud', 'PUN':'pun'}
print(lista_geo_zone)
i_geo = int(input('Inserisci il numero di posizione della zona geografica desiderata dalla lista: : ')) #inserire un controllo
geo_zone = lista_geo_zone[i_geo-1]
print('Hai selezionato: ', geo_zone)
nome_file = 'Anno '+str(anno)

# CREAZIONE DEL DATAFRAME CONTENENTE I DATI SELEZIONATI
df_gme = pd.DataFrame()



if os.path.exists(origin_path +'/'+ nome_file + '.xlsx') == True:     
    df_gme = pd.read_excel(origin_path + '/' + nome_file + '.xlsx', sheet_name=1)
else:
    print ('File non trovato: ')
nome_colonna_tempo = df_gme.columns[0]
nome_colonna_ore = df_gme.columns[1]

if os.path.exists(data_path) != True: 
    os.mkdir(data_path)
data_path = os.path.join(data_path, str(anno))
if os.path.exists(data_path) != True: 
    os.mkdir(data_path)
df_gme.to_csv(data_path +'/prezzi_gme.csv', index=False, decimal =',', sep=';')
df_prezzi = pd.concat([df_gme[nome_colonna_tempo], df_gme[nome_colonna_ore], df_gme['PUN'], df_gme[geo_zone]],axis=1)
df_pun = pd.DataFrame()
df_sici = pd.DataFrame()
for i in range (1,26):
    df_iter_pun = df_gme.loc[df_gme[nome_colonna_ore]==i, [nome_colonna_tempo, 'PUN']].set_index(nome_colonna_tempo).rename(columns = {'PUN':i})
    df_iter_sici = df_gme.loc[df_gme[nome_colonna_ore]==i, [nome_colonna_tempo, geo_zone]].set_index(nome_colonna_tempo).rename(columns = {geo_zone:i})
    df_pun = pd.concat([df_pun, df_iter_pun], axis=1)
    df_sici = pd.concat([df_sici, df_iter_sici], axis=1) 
df_pun.reset_index().to_csv(data_path+'/riepilogo_pun.csv', index=False, decimal =',', sep=';')
df_sici.reset_index().to_csv(data_path+'/riepilogo_pz_'+ dict_lista_zone_geo[geo_zone]+'.csv', index=False, decimal =',', sep=';')

print('Fine Operazioni')