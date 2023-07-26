#!/usr/bin/env python
# coding: utf-8

# ## Actualización de históricos
# 
# ##### Este notebook tiene como objetivo leer cada una de las hojas del excel de control de calidad de las estaciones meteorologicas de las que esta a cargo ICC y actulizar la información en los archivos historicos de cada estación

# In[160]:


import pandas as pd
import yaml
get_ipython().run_line_magic('matplotlib', 'inline')
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import glob
from pathlib import PureWindowsPath
from datetime import datetime
import re
import warnings
import os

warnings.filterwarnings("ignore")
# set the max columns to none
pd.set_option('display.max_columns', None)


# Obtén la ruta del directorio actual (donde se encuentra script.py)
directorio_actual = os.path.dirname(os.path.abspath(__file__))

# Construye la ruta al archivo YAML utilizando la ruta relativa
ruta_yaml = os.path.join(directorio_actual, 'data.yaml')



with open(ruta_yaml, 'r') as f:
    parametros=yaml.load(f,Loader=yaml.FullLoader)



# Lectura de los archivos historicos
# get data file names
path= parametros['global']['pc']+parametros['stage2']['path_historico']
filenames = glob.glob(path + "/*.xlsx")


# ### Limpieza de las bases historicas - se nombran columnas
# 
# ##### Se guardan las bases de cada estación en una lista

# In[162]:


dataframes=[]
data_clear=[]
for csv in filenames:
    name_df = re.split(r'[^a-zA-Z0-9\s]', csv)[-2]
    name_df = re.sub(r"\s+", '_', name_df).lower()
    dataframes.append(name_df)
    print(name_df)
    vars()[name_df] = pd.read_excel(csv, index_col=None)
    if vars()[name_df].shape[1]==9:
        vars()[name_df].columns = ["estacion", "fecha", "temperatura", 
        "radiacion", "humedad_relativa", "precipitacion",
         "velocidad_viento", "mojadura", "direccion_viento"]
        vars()[name_df]["presion_atm"]=np.nan
    else:
        vars()[name_df].columns = ["estacion", "fecha", "temperatura", 
        "radiacion", "humedad_relativa", "precipitacion",
         "velocidad_viento", "mojadura", "direccion_viento",
         "presion_atm"]
    #quitando caracteres especiales
    # para qitar caracteres especiales y/o espacios
    vars()[name_df] = vars()[name_df].replace(r'\W+', '', regex=True)
    vars()[name_df] = vars()[name_df].replace(r'\s+', ' ', regex=True)
    vars()[name_df] = vars()[name_df].replace("", np.nan, regex=True)
    vars()[name_df] = vars()[name_df].replace(" ", np.nan, regex=True)
    #formato
    vars()[name_df][[
        'temperatura',
        'radiacion',
        'precipitacion',
        'mojadura'
    ]] = vars()[name_df][[
        'temperatura',
        'radiacion',
        'precipitacion',
        'mojadura'
    ]].apply(pd.to_numeric)
    print('guardando')
    df1=pd.DataFrame(vars()[name_df])
    data_clear.append((df1))
    print(dataframes)
    
    

dfs = pd.concat(data_clear, axis=0, ignore_index=True)


dfs['fecha']=dfs.fecha.astype(str)
dfs['fecha']=dfs["fecha"].apply(lambda x: x[0:16])

"""
Se crea un diccionario con las abreviaturas de los nombres de cada estación

"""

dict_esta= {'cengicana':"CEN-CEN",'tehuantepec':"LUT-TEH", 'balsamo':"PAN-BAL", 'puyumate':"TBU-PUY", 'san_antonio_del_valle':"MAG-SAV", 'amazonas':"SAA-AMA",
             'trinidad':"SDT-TRI", 'irlanda':"TBU-IRL", 'bonanza':"LUT-BON", 'bougambilia':"MAG-BOU", 'naranjales':"PAG-NRJ", 'peten_oficina':"MAT-PEO",
             'costa_brava':"SDT-CBR", 'tulula': "TUL-TLA", 'lorena':"PAG-LOR" , 'san_rafael':"PAN-SRF", 'xoluta':"MAG-XOL", 'san_nicolas':"MAG-SNC",
             'trinidad_magdalena':"MAG-TMG", 'el_platanar':"ICC-PLT",  'la_giralda':"SAA-LGR", 'la_candelaria':"ICC-CDL",  'chiquirines':"ICC-CHI", 'providencia':"MAG-PVD", 
             'alamo':"ICC-ALA", 'cocales':"PAG-COC", 'concepcion':"ICC-CON", 'la_maquina':"SAA-LMQ", 'santa_teresa':"ICC-STA", 'yepocapa':"ICC-YEPO",
             'central_izalco':"CASSA-CIZ",'copal':"CASSA-CPL", 'chaparrastique':"ASSA-CPT", 'el_carmen':"CASSA-ECA", 'san_clemente':"CASSA-SCL", 'agua_santa':"CASSA-AGS",
             'pueblo_real':"ICC-PBR", 'monte_alegre':"ICC-MTA", 'tulula': "TUL-TLA", 
             'SOTZ':"'ICC-SOTZ", 'JOY':"ICC-JOY"
}


# ### Lectura de los datos con control de calidad


path2 =parametros['global']['pc']+parametros['stage1']['path_save_control']
filenames2 = glob.glob(path2 + "/*.csv")

dataframes2=[]
data_clear2=[]
for csv in filenames2:
    name_df = re.split(r'[^a-zA-Z0-9\s]', csv)[-2]
    name_df = re.sub(r"\s+", '_', name_df).lower()
    dataframes2.append(name_df)
    print(name_df)
    vars()[name_df] = pd.read_csv(csv, index_col=None)
    if vars()[name_df].shape[1]==9:
        vars()[name_df].columns = ["estacion", "fecha", "temperatura", 
        "radiacion", "humedad_relativa", "precipitacion",
         "velocidad_viento", "mojadura", "direccion_viento"]
        vars()[name_df]["presion_atm"]=np.nan
    else:
        vars()[name_df].columns = ["estacion", "fecha", "temperatura", 
        "radiacion", "humedad_relativa", "precipitacion",
         "velocidad_viento", "mojadura", "direccion_viento",
         "presion_atm"]
    #quitando caracteres especiales
    # para qitar caracteres especiales y/o espacios
    vars()[name_df] = vars()[name_df].replace(r'\W+', '', regex=True)
    vars()[name_df] = vars()[name_df].replace(r'\s+', ' ', regex=True)
    vars()[name_df] = vars()[name_df].replace("", np.nan, regex=True)
    vars()[name_df] = vars()[name_df].replace(" ", np.nan, regex=True)
    #formato
    vars()[name_df][[
        'temperatura',
        'radiacion',
        'precipitacion',
        'mojadura'
    ]] = vars()[name_df][[
        'temperatura',
        'radiacion',
        'precipitacion',
        'mojadura'
    ]].apply(pd.to_numeric)
    print('guardando')
    df2=pd.DataFrame(vars()[name_df])
    data_clear2.append((df2))
    print(dataframes2)


dfs2 = pd.concat(data_clear2, axis=0, ignore_index=True)

"""
Se crea un diccionario con las abreviaturas de los nombres de cada estación

"""

dict_esta= {'CEN':"CENCEN",'TEH':"LUTTEH", 'BAL':"PANBAL", 'PUY':"TBUPUY", 'SAV':"MAGSAV", 'AMA':"SAAAMA",
             'TRI':"SDTTRI", 'IRL':"TBUIRL", 'BON':"LUTBON", 'BOU':"MAGBOU", 'NRJ':"PAGNRJ", 'PEO':"MATPEO",
             'CBR':"SDTCBR", 'TLA': "TULTLA", 'LOR':"PAGLOR" , 'SRF':"PANSRF", 'XOL':"MAGXOL", 'SNC':"MAGSNC",
             'TMG':"MAGTMG", 'PLT':"ICCPLT",  'LGR':"SAALGR", 'CDL':"ICCCDL",  'CHI':"ICCCHI", 'PVD':"MAGPVD", 
             'ALA':"ICCALA", 'COC':"PAGCOC", 'CON':"ICCCON", 'LMQ':"SAALMQ", 'STA':"ICCSTA", 'YEPO':"ICCYEPO",
             'CIZ':"CASSACIZ",'CPL':"CASSACPL", 'CPT':"ASSACPT", 'ECA':"CASSAECA", 'SCL':"CASSASCL", 'AGS':"CASSAAGS",
             'PBR':"ICCPBR", 'MTA':"ICCMTA", 'TUL': "TULTLA", 'NAR': "PAGNRJ", 'LCD':"ICCCDL", 'CASSACPT':"ASSACPT", 'CTB':"SDTCBR",
             'SRAF':"PANSRF",'PUYU':"TBUPUY",'TEHU':"LUTTEH"
}

"""
Se realiza el reemplazo de las abrevituras a partir del diccionario creado

"""
dfs2.replace({"estacion": dict_esta},inplace=True)


def formato_fecha(fecha):
    partes_fecha= f"{fecha[:4]}-{fecha[4:6]}-{fecha[6:8]} {fecha[8:10]}:{fecha[10:]}"
    return partes_fecha



dfs2['fecha'] = dfs2['fecha'].apply(formato_fecha)

#### Cruzar dataframes

historico  = dfs.merge(dfs2, how='outer', on=['estacion', 'fecha', 'temperatura', 'radiacion', 'humedad_relativa',
       'precipitacion', 'velocidad_viento', 'mojadura', 'direccion_viento',
       'presion_atm'])


# In[232]:


##### Ubicación de la data historica actualizada 
os.chdir(parametros['global']['pc']+parametros['stage2']['path_hist_act_save'])


# In[233]:


### Guardar cada estacion es un archivo de excel diferente 

for i in historico.estacion.unique():
    
    df3=historico[historico['estacion']==i]
    df3.to_excel(f"{i}.xlsx",index=False)


# In[ ]:




