import pandas as pd
from datetime import datetime, timedelta
import sqlalchemy
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker
from configurazione.init import *
import matplotlib
import time
import sys
matplotlib.use("TkAgg")
plt.ion()

def get_today_gme_dataframe_from_database(user, password, host, db, db_table, db_col):
    now = datetime.now()
    today = datetime.strftime(now, '%Y%m%d')
    url = 'mysql+pymysql://'+user+':'+password+'@'+host+'/'+db
    connection = sqlalchemy.create_engine(url)
    query = "SELECT * FROM "+str(db_table)+" WHERE "+str(db_col)+" = '" +str(today)+"'"
    df = pd.read_sql(query, connection)
    return(df)

def get_gme_dataframe_from_database(user, password, host, db, db_table, db_col, data):
    url = 'mysql+pymysql://'+user+':'+password+'@'+host+'/'+db
    connection = sqlalchemy.create_engine(url)
    query = "SELECT * FROM "+str(db_table)+" WHERE "+str(db_col)+" >= '" +str(data)+"'"
    df = pd.read_sql(query, connection)
    return(df)

def extract_data_from_dataframe(df_iniziale, mercato, data, boxplot):   
    lista_minimi = []
    lista_massimi = []
    lista_medie = []
    df_finale=pd.DataFrame()
    xlabel_grafico = []
    if boxplot =='sì':
        lista_valori_boxplot = []
    for giorno in data:
        if len(data)>300:
            xlabel_grafico.append(str(giorno)[6:]+'/'+str(giorno)[4:6]+'/'+str(giorno)[0:4])
        else:
            xlabel_grafico.append(str(giorno)[6:]+'/'+str(giorno)[4:6])
        df_giorno_iterativo = df_iniziale.loc[
            df_iniziale['Data'] == int(giorno)
        ]
        df_finale=pd.concat([df_finale, df_giorno_iterativo], axis =0) ## --> per plot specifico (frequenze)
        lista_minimi.append(df_giorno_iterativo[mercato].min())
        lista_massimi.append(df_giorno_iterativo[mercato].max())
        lista_medie.append(df_giorno_iterativo[mercato].mean())
        if boxplot =='sì':
            lista_valori_boxplot.append(df_giorno_iterativo[mercato].tolist())
    y = df_finale[mercato].tolist()
    x = np.linspace(1, len(y), len(y))
    x_lista = np.linspace(1, len(lista_minimi), len(lista_minimi))
    
    minimo_ass = str(round(df_finale[mercato].min(),2))
    massimo_ass = str(round(df_finale[mercato].max(),2))
    val_medio = str(round(df_finale[mercato].mean(),2))
    
    
    if boxplot =='sì':
        return(lista_minimi,lista_massimi,lista_medie,x,y,x_lista,xlabel_grafico, minimo_ass, massimo_ass, val_medio, lista_valori_boxplot)
    else:
        return(lista_minimi,lista_massimi,lista_medie,x,y,x_lista,xlabel_grafico, minimo_ass, massimo_ass, val_medio)

def main_plotter(mercato):
    #DATAFRAME PRINCIPALE
    today = datetime.now()
    data =  today-timedelta(days = 4*365)
    data = datetime.strftime(data, '%Y%m%d')
    df = get_gme_dataframe_from_database(db_user, db_password, db_host, db_gme, db_table_gme, db_colonna_giorno_gme, data)
    lista_date = df['Data'].unique().tolist()

    #DATAFRAME GIORNALIERO + DATI
    oggi = [datetime.strftime(today, '%Y%m%d')]
    minimi_giorno, massimi_giorno, medie_giorno, x_day, y_day, xday_lista, xlabel_giorno, minimo_oggi, massimo_oggi, val_medio_oggi  = extract_data_from_dataframe(df, mercato, oggi, boxplot='no')

    settimana = lista_date[-8:]
    minimi_week, massimi_week, medie_week, x_week, y_week, xweek_lista, xlabel_week, minimo_week, massimo_week, val_medio_week  = extract_data_from_dataframe(df, mercato, settimana, boxplot='no')

    mese = lista_date[-31:]
    minimi_mese, massimi_mese, medie_mese, x_mese, y_mese, xmese_lista, xlabel_mese, minimo_mese, massimo_mese, val_medio_mese, lista_valori_boxplot  = extract_data_from_dataframe(df, mercato, mese, boxplot='sì')

    lungo_periodo = lista_date[-732:]
    minimi_long_per, massimi_long_per, medie_long_per, x_long_per, y_long_per, xlong_lista, xlabel_long_per, minimo_long_per, massimo_long_per, val_medio_long_per  = extract_data_from_dataframe(df, mercato, lungo_periodo, boxplot='no')

    mese_precedente = lista_date[-62:-31]
    minimi_mese_prec, massimi_mese_prec, medie_mese_prec, x_mese_prec, y_mese_prec, xmeseprec_lista, xlabel_mese_prec, minimo_mese_prec, massimo_mese_prec, val_medio_mese_prec = extract_data_from_dataframe(df, mercato, mese_precedente, boxplot='no')


    ## RAPPRESENTAZIONE
    global fig
    fig = plt.figure(figsize=(15,15))

    plot1 = plt.subplot2grid((3, 4), (0, 0), colspan=2) 
    plot2 = plt.subplot2grid((3, 4), (0, 2), colspan=1) 
    plot3 = plt.subplot2grid((3, 4), (1, 0), colspan=3)
    plot4 = plt.subplot2grid((3, 4), (2, 0), colspan=4)
    plot5 = plt.subplot2grid((3, 4), (0, 3), colspan=1, rowspan=2)

    #GIORNALIERO

    plot1.bar(x_day, y_day, width=0.6)
    plot1.grid(True)
    plot1.set_xlabel("ORA")
    plot1.set_ylabel("PREZZO [€/MWh]")
    plot1.set_title('PREZZO '+mercato+' GME-MGP - '+ str(oggi[0]), y=0.99, pad=-10, bbox={'facecolor': 'w', 'alpha': 1, 'pad': 1.5})
    plot1.set_ylim(0, max(y_day)+60)
    x_day_label = []
    for x in x_day:
        x_day_label.append(int(x))
    plot1.set_xticks(x_day, x_day_label)
    plot1.tick_params(axis='x', rotation=0, labelsize='xx-small')
    rects = plot1.patches
    for rect, label in zip(rects, y_day):
        height = rect.get_height()
        plot1.text(
            rect.get_x() + rect.get_width() / 2, height +5 , label, ha="center", va="bottom", size=8,rotation=90, color = 'red'
        )

    #SETTIMANALE
    plot2.plot (xweek_lista, minimi_week, label = mercato + ' minimo')
    plot2.plot (xweek_lista, medie_week, color='red', alpha=1, label = mercato +' medio')
    plot2.plot (xweek_lista, massimi_week, label = mercato + ' massimo')
    plot2.fill_between(xweek_lista, massimi_week, medie_week, color='orange', alpha = 0.25)
    plot2.fill_between(xweek_lista, minimi_week, medie_week, color='red', alpha =0.25)
    plot2.fill_between(xweek_lista, minimi_week, color='blue', alpha =0.25)
    plot2.grid(True)
    plot2.legend()
    plot2.set_xlabel("GIORNO")
    plot2.set_ylabel("PREZZO [€/MWh]")
    plot2.set_ylim(0, max(massimi_week)+60)
    plot2.set_title('RIEPILOGO SETTIMANALE '+mercato, y=0.99, pad=-10, bbox={'facecolor': 'w', 'alpha': 1, 'pad': 1.5})
    plot2.set_xticks(xweek_lista, xlabel_week)
    plot2.tick_params(axis='x', rotation=0, labelsize='xx-small') # set tick rotation

    #MENSILE
    plot3.boxplot(lista_valori_boxplot, widths = 0.3)
    plot3.grid(True)
    plot3.set_xlabel("GIORNO")
    plot3.set_ylabel("PREZZO [€/MWh]")
    plot3.set_title('RIEPILOGO MENSILE '+mercato, y=0.99, pad=-10, bbox={'facecolor': 'w', 'alpha': 1, 'pad': 1.5})
    plot3.set_xticks(xmese_lista, xlabel_mese)
    plot3.tick_params(axis='x', rotation=0, labelsize='xx-small') # set tick rotation

    #LUNGO PERIODO
    plot4.plot (xlong_lista, minimi_long_per, label = mercato+' minimo')
    plot4.plot (xlong_lista, medie_long_per, color='red', alpha=1, label = mercato+' medio')
    plot4.plot (xlong_lista, massimi_long_per, label = mercato+' massimo')
    plot4.fill_between(xlong_lista, massimi_long_per, medie_long_per, color='orange', alpha = 0.25)
    plot4.fill_between(xlong_lista, minimi_long_per, medie_long_per, color='red', alpha =0.25)
    plot4.legend()
    plot4.set_xlabel("GIORNO")
    plot4.set_ylabel("PREZZO [€/MWh]")
    plot4.set_title('ANDAMENTO '+mercato+' - 2 ANNI', x = 0.7, y=0.99, pad=-10, bbox={'facecolor': 'w', 'alpha': 1, 'pad': 1.5})
    plot4.tick_params(axis='x', rotation=90, labelsize='xx-small') # set tick rotation
    plot4.set_xticks(xlong_lista, xlabel_long_per)
    plot4.xaxis.set_major_locator(ticker.MaxNLocator(61))
    plot4.yaxis.set_major_locator(ticker.MaxNLocator(10))
    plot4.yaxis.set_minor_locator(ticker.MaxNLocator(20))
    plot4.grid(which='major', color='black', linewidth=0.5)
    plot4.grid(which='minor', color='darkgrey', linestyle=':', linewidth=0.5)

    #RIEPILOGO TESTO
    plot5.xaxis.set_major_locator(ticker.NullLocator())
    plot5.yaxis.set_major_locator(ticker.NullLocator())
    plot5.text(0.25,0.95,'VALORI ODIERNI', style='normal', size='large', weight='bold', color='red') #, bbox={'facecolor': 'b', 'alpha': 0.5, 'pad': 10})
    plot5.text(0.05,0.9,mercato+' medio: '+ val_medio_oggi +' €/MWh', style='normal', size='large', weight='bold') 
    plot5.text(0.05,0.85,mercato+' max: '+ massimo_oggi+' €/MWh', style='normal', size='large', weight='bold') 
    plot5.text(0.05,0.8,mercato+' min: '+ minimo_oggi+' €/MWh', style='normal', size='large', weight='bold') 

    plot5.text(0.2,0.7,'VALORI SETTIMANALI', style='normal', size='large', weight='bold', color='red') #, bbox={'facecolor': 'b', 'alpha': 0.5, 'pad': 10})
    plot5.text(0.05,0.65,mercato+' medio: '+ val_medio_week+' €/MWh', style='normal', size='large', weight='bold') 
    plot5.text(0.05,0.6,mercato+' max: '+ massimo_week+' €/MWh', style='normal', size='large', weight='bold') 
    plot5.text(0.05,0.55,mercato+' min: '+ minimo_week+' €/MWh', style='normal', size='large', weight='bold') 

    plot5.text(0.25,0.45,'VALORI MENSILI', style='normal', size='large', weight='bold', color='red') #, bbox={'facecolor': 'b', 'alpha': 0.5, 'pad': 10})
    plot5.text(0.05,0.4,mercato+' medio: '+ val_medio_mese+' €/MWh', style='normal', size='large', weight='bold') 
    plot5.text(0.05,0.35,mercato+' max: '+ massimo_mese+' €/MWh', style='normal', size='large', weight='bold') 
    plot5.text(0.05,0.3,mercato+' min: '+ minimo_mese+' €/MWh', style='normal', size='large', weight='bold') 

    plot5.text(0.1,0.2,'VALORI MESE PRECEDENTE', style='normal', size='large', weight='bold', color='red') #, bbox={'facecolor': 'b', 'alpha': 0.5, 'pad': 10})
    plot5.text(0.05,0.15,mercato+' medio: '+ val_medio_mese_prec+' €/MWh', style='normal', size='large', weight='bold') 
    plot5.text(0.05,0.1,mercato+' max: '+ massimo_mese_prec+' €/MWh', style='normal', size='large', weight='bold') 
    plot5.text(0.05,0.05,mercato+' min: '+ minimo_mese_prec+' €/MWh', style='normal', size='large', weight='bold') 
    
    fig.canvas.draw()
    fig.canvas.flush_events()
    fig.savefig(export_path+'/'+str(today)[0:11]+'_'+mercato+'.png')
    return()


def gme_datalogger(mercato):
    now = datetime.now()
    today = str(now)[0:11]
    while True:
        if today[0:11] != str(now)[0:11]: 
            plt.close(fig)
            today = str(now)[0:11]

        while today[0:11] == str(now)[0:11]:
            now = datetime.now()
            print('GME plotter ', mercato, ' inserimento ora: ', now)
            main_plotter(mercato)
            time.sleep(14400)
            plt.close(fig)
    return
    
while True:
    try:
        mercato = str(sys.argv[1])
        gme_datalogger(mercato)
    except Exception as error:
        sys.exit(str(error))
    break