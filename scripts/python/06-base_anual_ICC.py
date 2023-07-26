#!/usr/bin/env python
# coding: utf-8

# ## Agregaciones Anuales
# 
# #### Este notebook tiene como objetivo generar las tablas de las variables climaticas anuales


## Libreías generales
import numpy as np
import yaml
import pandas as pd
import datetime
import os
from dateutil.relativedelta import relativedelta


#comando para visualizar todas las columnas del df
pd.pandas.set_option('display.max_columns', None)


# Obtén la ruta del directorio actual (donde se encuentra script.py)
directorio_actual = os.path.dirname(os.path.abspath(__file__))

# Construye la ruta al archivo YAML utilizando la ruta relativa
ruta_yaml = os.path.join(directorio_actual, 'data.yaml')


with open(ruta_yaml, 'r') as f:
    parametros=yaml.load(f,Loader=yaml.FullLoader)


### Se carga el archivo del calculo de las variables semanales 

df_semanal = pd.read_parquet(parametros['global']['pc']+parametros['stage6']['path_semanal'])


# In[6]:


df_anual=df_semanal.groupby(['estrato','año']).agg(
    Radic_solar_prome_anual=('rad_diar_prom_semanal','sum'),
    brillo_solar_acum_anual=('n_brillo_solar_acum','sum'),
    precp_max_anual= ('prec_acumulada','max'),
    ampl_term_acmu_anual= ('ampl_term_prom_semanal','sum')
    ).reset_index().round(2)
df_anual


##### Ubicación de la tabla general
os.chdir(parametros['global']['pc'])

"""Se guarda la  base de datos con las variables calculadas anualmente, en este caso se guarda en formato parquet dado
que este formato es mas liviano que descargar la bae en un excel, pero tambien es posible guardar en formato excel """

df_anual.to_parquet('anual_complete.parquet')

#df_anual.to_excel('anual_complete.excel')





