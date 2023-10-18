from configurazione.init import *
import ftplib
from datetime import datetime
import os


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

init_path = '/home/episciotta/Documenti/SVILUPPO'
file_path = '/home/episciotta/Documenti/SVILUPPO/dati_grezzi/elaborazione_dati_energia/curve_gme/ftp/MercatiElettrici/MGP_Prezzi'

change_directory(file_path)

ftpc = ftplib.FTP()
ftpc.connect(ftp_host, ftp_port)
ftpc.login(nome_utente, password)




ftp_path = ftpc.pwd()
ftpc.cwd('MercatiElettrici')
ftpc.cwd('MGP_Prezzi')
xml_file_list = []
ftpc.retrlines('LIST', xml_file_list.append) ## restituisce il contenuto della cartella 



for file in xml_file_list:
    if not os.path.isfile(file[-21::]):
        file_locale = os.path.join(file_path, file[-21::])
        #print('Download File: ', file[-21::])
        with open(file_locale, "wb") as f:
            ftpc.retrbinary("RETR " + file[-21::], f.write)
print('Fine Operazioni')
            
ftpc.quit()