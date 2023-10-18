## DOWNLOAD VIA FTP DEI PREZZI ENERGETICI PUBBLICATI DAL GME E UPLOAD DEI VALORI SU APPOSITO DATABASE

from configurazione.init import *
import ftplib
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
import time
import os
import sys
import pandas as pd
import numpy as np
import ftplib
import sqlalchemy

def change_directory(path):
    try:
        os.chdir(path)
        print("Current working directory: {0}".format(os.getcwd()))
    except FileNotFoundError:
        print("Directory: {0} does not exist".format(path))
    except NotADirectoryError:
        print("{0} is not a directory".format(path))
    except PermissionError:
        print("You do not have permissions to change to {0}".format(path))
    return

def download_ftp():
    change_directory(file_path)
    ftpc = ftplib.FTP()
    ftpc.connect(ftp_host, ftp_port)
    ftpc.login(ftp_user, ftp_password)
    ftp_path = ftpc.pwd()
    ftpc.cwd('MercatiElettrici')
    ftpc.cwd('MGP_Prezzi')
    xml_file_list = []
    ftpc.retrlines('LIST', xml_file_list.append) ## restituisce il contenuto della cartella 
    for file in xml_file_list:
        if not os.path.isfile(file[-21::]):
            file_locale = os.path.join(file_path, file[-21::])
            print('Download File: ', file[-21::])
            with open(file_locale, "wb") as f:
                ftpc.retrbinary("RETR " + file[-21::], f.write)
    print('Download FTP completato')
    ftpc.quit()
    return

def check_last_db_entry():
    global today 
    global today_str 
    today = datetime.today()
    today_str = str(today.strftime('%Y%m%d'))[0:19]
    query = "SHOW TABLES LIKE '"+str(db_table)+"'"
    cursor = connection.execute(query)
    result = cursor.fetchall()
    if result: 
        query = ("SHOW COLUMNS FROM `{}` LIKE '{}'").format(db_table, db_colonna_temporale)
        cursor = connection.execute(query)
        result = cursor.fetchall()
        if result:            
            query = "SELECT "+str(db_colonna_temporale)+" FROM "+str(db_table)+" WHERE EXISTS(SELECT * FROM "+str(db_table)+" WHERE "+str(db_colonna_temporale)+" <= '"+str(today_str)+"') ORDER BY "+str(db_colonna_temporale)+" DESC LIMIT 1"
            cursor = connection.execute(query)
            last_entry = cursor.fetchall()
            print(last_entry)
            if last_entry:
                print('ultimo valore su db: ', (last_entry[0])[0])
            else:
                print('db vuoto')
            if connection:
                connection.close()
    else:
        print('creazione tabella')
        query = "CREATE TABLE `"+str(db_database)+"`.`"+str(db_table)+"` (`id` INT NOT NULL AUTO_INCREMENT, `Data` INT NOT NULL, `Mercato` VARCHAR(45) NOT NULL, `Ora` INT NOT NULL, PRIMARY KEY (`id`), UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE)"
        cursor = connection.execute(query)
        if connection:
            connection.close()
        last_entry = []
    return(last_entry)

def xml_data_mining (file):
    print('Elaborazione file: ', file)
    with open(file_path+'/'+file, 'r') as f:
        xml_soup = BeautifulSoup(f, features="xml")
    lista_nomi = []
    lista_elementi = []

    df = pd.DataFrame()
    ## LETTURA DI TUTTI GLI xs:element DEL FILE XML ED ESTRAZIONE DELLE CHIAVI(NOMI)
    for xs_element in xml_soup.find_all('xs:element'):
        lista_elementi.append(xs_element)
        lista_nomi.append(xs_element['name'])
    ## ESTRAZIONE DEI VALORI PER CIASCUNA CHIAVE. ESCLUSIONE DELLE PRIME DUE CHIAVI [2::] NON UTILI
    for nome in lista_nomi[2::]:
        str_valori_numerici = []
        lista_valori = []
        lista_valori = xml_soup.find_all(nome)
        if lista_valori:
            for xs_element in lista_valori: ##la lista valori per E_SD è vuota e tiene in memoria il valore precedente
                val = xs_element.string
                try:
                    converted_val = str(val.encode('utf-8'))[2:-1]
                    converted_val=converted_val.replace(',', '.')
                except:
                    pass
                try:
                    converted_val = int(converted_val)
                except:
                    try:
                        converted_val = float(converted_val)
                    except:
                        pass
                str_valori_numerici.append(converted_val)
                colonna_dataframe = pd.Series(data = str_valori_numerici, name = nome)
            df = pd.concat([df, colonna_dataframe], axis = 1)
    return(df)

    
def column_check(df):
    url = 'mysql+pymysql://'+db_user+':'+db_password+'@'+db_host+'/'+db_database
    engine = sqlalchemy.create_engine(url)
    connection = engine.connect()
    nuove_colonne =[]
    for colonna in df.columns.tolist():
        query = ("SHOW COLUMNS FROM `{}` LIKE '{}'").format(db_table, str(colonna))
        cursor = connection.execute(query)
        result = cursor.fetchall()
        if not result:
            if colonna not in nuove_colonne:
                nuove_colonne.append(colonna)
    for colonna in nuove_colonne:
        query = "ALTER TABLE "+str(db_table)+" ADD `" +str(colonna)+ "` FLOAT"
        cursor = connection.execute(query)
    connection.close()
    return
    
def elenco_file_scaricati(file_path):
    lista_file = os.listdir(file_path)
    lista_file.sort()
    for file in lista_file:
        if not 'xml' in file:
            lista_file.remove(file)
    return(lista_file)

def connect_to_mysql():
    global engine
    global connection
    try:
        url = 'mysql+pymysql://'+db_user+':'+db_password+'@'+db_host+'/'+db_database
        engine = sqlalchemy.create_engine(url)
        connection = engine.connect()
    except Exception as error:
        sys.exit(str(error))
    return

def ottieni_ultimo_valore_su_database():    
    try:
        last_entry = check_last_db_entry()
    except Exception as error:
        print("Non è stato possibile interrogare il database")
        if connection:
            connection.close()
        sys.exit(str(error))
    return(last_entry)

def upload_dati_su_database(last_entry, lista_file):
    print("Avvio dell'upload dei dati su database")
    if last_entry:
        for file in lista_file:
            if str(last_entry[0][0]) in file:
                last_entry_index = lista_file.index(file)
        if last_entry_index < len(lista_file):
            for file in lista_file[last_entry_index+1::]:
                print(file)
                df = xml_data_mining (file)
                column_check(df)
                try:
                    df.to_sql(db_table, engine, index = False, if_exists= 'append')  
                except Exception as error:
                    if connection: 
                        connection.close()
                    ("Non è stato possibile completare l'upload su db")
                    sys.exit(str(error))
    else:
        for file in lista_file:
            df = xml_data_mining (file)
            column_check(df)
            try:
                df.to_sql(db_table, engine, index = False, if_exists= 'append')  
            except Exception as error:
                if connection:
                    connection.close()
                ("Non è stato possibile completare l'upload su db")
                sys.exit(str(error))
    print('Upload dati su database completato')
    return

def datalogger_gme():
    download_ftp()
    lista_file = elenco_file_scaricati(file_path) 
    connect_to_mysql()
    last_entry = ottieni_ultimo_valore_su_database()
    upload_dati_su_database(last_entry, lista_file)
    return

while True:
    try:
        datalogger_gme()
    except Exception as error:
        sys.exit(str(error))