#!/usr/bin/env python
# coding: utf-8

# ## Boletin semanal asociación de productores independientes de banano
# 
# ##### Este notebook tiene como objetivo construir la tabla y graficos de reporte que realiza ICC

# In[1]:


## Libreías generales
import numpy as np
import pandas as pd
import datetime
import os
import yaml
from dateutil.relativedelta import relativedelta


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
     

dfs_diario=pd.read_parquet(parametros['global']['pc']+parametros['stage5']['path_diario_complete'])


##Lectura archivo de la información por estación

info_estaciones = pd.read_excel(parametros['global']['pc']+parametros['stage5']['path_info_estaciones'])


###Limpieza de las columna de codigo
info_estaciones.Codigo =info_estaciones.Codigo.replace(r'\W+', '', regex=True)
info_estaciones.Codigo = info_estaciones.Codigo.replace(r'\s+', ' ', regex=True)
info_estaciones.Codigo = info_estaciones.Codigo.replace("", np.nan, regex=True)
info_estaciones.Codigo= info_estaciones.Codigo.replace(" ", np.nan, regex=True)

### Columnas que se necesitan
info_estrato =info_estaciones[['Estacion','Codigo','t','Altitud\n(msnm)','LUGAR']]
info_estrato.columns = ['nombre_estacion','estacion','estrato','Altitud\n(msnm)','Ubicacion']


df_semanal_estacion = dfs_diario.groupby(['estacion','semana','año']).agg(
    temp_min_semanal=('temperatura_min_diaria', 'mean'),
    temp_max_semanal=('temperatura_max_diaria', 'mean'),
    temp_promedio_semanal=('temperatura_promedio_diaria', 'mean'),
    rad_diar_prom_semanal=('radiacion_diaria_promedio', 'mean'),
    ampl_term_prom_semanal = ('amplitud_termica', 'mean'),
    hume_rela_med_semanal= ('humedad_relativa_media_diaria','mean'),
    Veloc_viento_prom_max_semanal= ('velocidad_viento_max_diaria', 'mean'),
    Veloc_viento_media_semanal= ('velocidad_viento_media_diario', 'mean' ),
    max_lluvia=('lluvia_diaria', 'max'),
    acumu_lluvia=('lluvia_diaria', 'sum'),
    suma_FAO= ('FAO-Penman-Montieth','sum')

).reset_index().round(2).astype({'semana':int, 'año':int})

df_semanal_estacion


# In[6]:


df_semanal_estacion['Balance_precipitacion-evapotranspiracion']=df_semanal_estacion.acumu_lluvia-df_semanal_estacion.suma_FAO



df_semanal_estacion=df_semanal_estacion.merge(info_estrato, how='right', on=['estacion'])


Boletin_banano=df_semanal_estacion[['nombre_estacion','Altitud\n(msnm)','Ubicacion','semana', 'año','temp_min_semanal','temp_promedio_semanal','temp_max_semanal','hume_rela_med_semanal',
                                    'acumu_lluvia','suma_FAO','Balance_precipitacion-evapotranspiracion']]
Boletin_banano.columns = ['Estación','Altitud(msnm)','Ubicacion','semana', 'año','temp_min_semanal','temp_promedio_semanal','temp_max_semanal','hume_rela_med_semanal',
                                    'Precipitacion acumulada','Evapotranspiración_acumulada','Balance_precipitacion-evapotranspiracion']


"""
se elimnan las estaciones 'PLT':"ICCPLT", 'STA':"ICCSTA", 'PBR':"ICCPBR" y 'YEPO':"ICCYEPO", 
dado que los reportes que realizan desde ICC solo incluyen las estaciones de zonas cañeras, 
por lo tanto se reportan 28 estaciones 

"""

to_remove = Boletin_banano.loc[Boletin_banano.Estación.isin(['Santa Teresa ', 'FCA Yepocapa', 'Central Izalco ', 'Copal ', 'Chaparrastique', 'El Carmen', 'San Clemente', 'Agua Santa', 'Pueblo Real ','El Platanar'])].index

Boletin_banano.drop(to_remove, inplace=True)


# In[20]:


Semana= int(parametros['stage8']['semana_gb'])
periodo=int(parametros['stage8']['periodo'])


week_banano=Boletin_banano[(Boletin_banano.semana==Semana)&(Boletin_banano.año==periodo)]

##### Ubicación de tabla dsel boletin climatico semanal
os.chdir(parametros['global']['pc'])



### Guardar tabla en excel 
week_banano.to_excel('boletin_banano.xlsx')



