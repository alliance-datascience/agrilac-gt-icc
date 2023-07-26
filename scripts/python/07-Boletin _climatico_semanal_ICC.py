#!/usr/bin/env python
# coding: utf-8

# ## Boletín climático semanal ICC
# 
# ##### Este notebook tiene como objetivo construir la tabla de reporte que realiza ICC

# In[1]:


## Libreías generales
import numpy as np
import pandas as pd
import datetime
import os
import yaml

from dateutil.relativedelta import relativedelta

import matplotlib.pyplot as plt
from matplotlib import cm
import matplotlib

# Configuración warnings
# ==============================================================================
import warnings
warnings.filterwarnings('ignore')

#comando para visualizar todas las columnas del df
pd.pandas.set_option('display.max_columns', None)


# Obtén la ruta del directorio actual (donde se encuentra script.py)
directorio_actual = os.path.dirname(os.path.abspath(__file__))

# Construye la ruta al archivo YAML utilizando la ruta relativa
ruta_yaml = os.path.join(directorio_actual, 'data.yaml')


with open(ruta_yaml, 'r') as f:
    parametros=yaml.load(f,Loader=yaml.FullLoader)
    
### Lectura de archivo diario 

dfs_diario=pd.read_parquet(parametros['global']['pc']+parametros['stage5']['path_diario_complete'])



### Lectura de archivo semanal

dfs_semanal=pd.read_parquet(parametros['global']['pc']+parametros['stage6']['path_semanal'])


### Lectura de archivo anual

dfs_anual=pd.read_parquet(parametros['global']['pc']+parametros['stage7']['path_anual'])


dfs_semanal_agg= dfs_diario.groupby(['estrato','semana','año']).agg(

    hume_rela_min_semanal= ('humedad_relativa_min_diaria','mean'),
    hume_rela_max_semanal= ('humedad_relativa_max_diaria','mean'),
    hume_rela_media_semanal=('humedad_relativa_media_diaria','mean'),
    ETP_media= ('FAO-Penman-Montieth','mean')
    

).reset_index().round(2).astype({'semana':int, 'año':int})



"""
Se cruza la base con los periodos seleccionados junto con las nuevas variables 
"""
dfs_semanal=dfs_semanal.merge(dfs_semanal_agg, how='right', on=['estrato','semana','año'])



Semana= int(parametros['stage7']['semana'])



week_report = dfs_semanal[dfs_semanal['semana'] == Semana]
recent_year = week_report['año'].max()
sele_period = list(range(recent_year, recent_year - 4, -1))
week_report = week_report[week_report['año'].isin(sele_period)]


# In[12]:


Boletin=['estrato','semana', 'año','temp_min_semanal', 'temp_promedio_semanal',
         'temp_max_semanal','ampl_term_prom_semanal','hume_rela_min_semanal',
         'hume_rela_media_semanal','hume_rela_max_semanal','rad_diar_prom_semanal',
         'n_brillo_solar_acum','preci_semana','prec_acumulad_anual','Veloc_viento_media_semanal','Veloc_viento_prom_max_semanal','ETP_media']


# In[13]:


week_report=week_report[Boletin]


##### Ubicación de tabla dsel boletin climatico semanal
os.chdir(parametros['global']['pc'])



week_report.to_excel('tabla_boletin_climatico_semanal.xlsx')

