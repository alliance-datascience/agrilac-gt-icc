#!/usr/bin/env python
# coding: utf-8

# # Lectura de los datos historicos actualizados del 2007 a la fecha

# In[1]:


import yaml
import pandas as pd
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


# get data file names
#path ="D:/OneDrive - CGIAR/Proyectos/AgriLAC-ICC/ICC/historico_actual"
path= parametros['global']['pc']+parametros['stage2']['path_hist_act_save']
filenames = glob.glob(path + "/*.xlsx")
filenames



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
    


# In[5]:


dfs = pd.concat(data_clear, axis=0, ignore_index=True)

##### Ubicación de la data completa
os.chdir(parametros['global']['pc'])


dfs.to_parquet('icc_station_actual.parquet')


# In[ ]:




