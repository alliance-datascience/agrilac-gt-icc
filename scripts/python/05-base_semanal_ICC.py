#!/usr/bin/env python
# coding: utf-8

# ## Agregaciones Semanales 
# 
# #### Este notebook tiene como objetivo generar las tablas de las variables climaticas por semana

## Libreías generales
import numpy as np
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import yaml
import os


#comando para visualizar todas las columnas del df
pd.pandas.set_option('display.max_columns', None)


# Obtén la ruta del directorio actual (donde se encuentra script.py)
directorio_actual = os.path.dirname(os.path.abspath(__file__))

# Construye la ruta al archivo YAML utilizando la ruta relativa
ruta_yaml = os.path.join(directorio_actual, 'data.yaml')


with open(ruta_yaml, 'r') as f:
    parametros=yaml.load(f,Loader=yaml.FullLoader)


#### Carga de información de las estaciones
info_estaciones = pd.read_excel(parametros['global']['pc']+parametros['stage5']['path_info_estaciones'])


# In[6]:



####Limpieza de la base  de datos para eliminar caracteres especiales 

info_estaciones.Codigo =info_estaciones.Codigo.replace(r'\W+', '', regex=True)
info_estaciones.Codigo = info_estaciones.Codigo.replace(r'\s+', ' ', regex=True)
info_estaciones.Codigo = info_estaciones.Codigo.replace("", np.nan, regex=True)
info_estaciones.Codigo= info_estaciones.Codigo.replace(" ", np.nan, regex=True)


### Se seleccionan de la base info_estaciones, las columnas que se van a utilizar 

info_estrato =info_estaciones[['Estacion','Codigo','t']]
info_estrato.columns = ['nombre_estacion','estacion','estrato']


"""
Se crea un diccionario con las abreviaturas de los nombres de cada estación

"""

dict_esta= {'CEN':"CENCEN",'TEH':"LUTTEH", 'BAL':"PANBAL", 'PUY':"TBUPUY", 'SAV':"MAGSAV", 'AMA':"SAAAMA",
             'TRI':"SDTTRI", 'IRL':"TBUIRL", 'BON':"LUTBON", 'BOU':"MAGBOU", 'NRJ':"PAGNRJ", 'PEO':"MATPEO",
             'CBR':"SDTCBR", 'TLA': "TULTLA", 'LOR':"PAGLOR" , 'SRF':"PANSRF", 'XOL':"MAGXOL", 'SNC':"MAGSNC",
             'TMG':"MAGTMG", 'PLT':"ICCPLT",  'LGR':"SAALGR", 'CDL':"ICCCDL",  'CHI':"ICCCHI", 'PVD':"MAGPVD", 
             'ALA':"ICCALA", 'COC':"PAGCOC", 'CON':"ICCCON", 'LMQ':"SAALMQ", 'STA':"ICCSTA", 'YEPO':"ICCYEPO",
             'CIZ':"CASSACIZ",'CPL':"CASSACPL", 'CPT':"ASSACPT", 'ECA':"CASSAECA", 'SCL':"CASSASCL", 'AGS':"CASSAAGS",
             'PBR':"ICCPBR", 'MTA':"ICCMTA", 'TUL': "TULTLA", 'NAR': "PAGNRJ", 'LCD':"ICCCDL", 'CASSACPT':"ASSACPT"
}


### Se carga el archivo que se guardo de los calculos de las variables diarias

df_diario = pd.read_parquet(parametros['global']['pc']+parametros['stage5']['path_diario_complete'])


# In[10]:


"""
Se realiza el reemplazo de las abrevituras a partir del diccionario creado

"""
df_diario.replace({"estacion": dict_esta},inplace=True)


# ## Join
# #### Se cruzan las bases de datos de información de las estaciones junto con la base de datos general diaria 


"""
Se cruza la base de info de las estaciones y la base de datos completa para agregar el estrato 

"""
df_diario=df_diario.merge(info_estrato, how='right', on=['estacion', 'estrato'])


# In[13]:


"""
Se eliminan las estaciones del salvador dado que no se incluyen en los boletines que realizan en ICC las cuales son: 
CIZ:CASSACIZ, 'CPL':"CASSACPL", 'SCL':"CASSASCL", 'AGS':"CASSAAGS",'CPT':"ASSACPT" y 'ECA':"CASSAECA"
A su vez se elimnan las estaciones 'PLT':"ICCPLT", 'STA':"ICCSTA", 'PBR':"ICCPBR" y 'YEPO':"ICCYEPO", 
dado que los reportes que realizan desde ICC solo incluyen las estaciones de zonas cañeras, 
por lo tanto se reportan 28 estaciones 

"""

to_remove = df_diario.loc[df_diario.estacion.isin(["CASSACIZ","CASSACPL","CASSASCL","CASSAAGS","CASSACPT",
                           "CASSAECA","ICCPLT","ICCPBR","ICCSTA","ICCYEPO"])].index

df_diario.drop(to_remove, inplace=True)


# ### Base de datos brillo solar

# In[14]:


'Se crea una base de datos que contiene por estación el acumulado por semana de brillo solar  '

Brillo_solar= df_diario.groupby(['estacion', 'semana','año','estrato']).agg(n_brillo_solar_acum=('n', 'sum')).round(2).reset_index()
Brillo_solar=Brillo_solar.astype({'semana':int, 'año':int})
#Brillo_solar[(Brillo_solar.estacion=='CENCEN')& (Brillo_solar.año==2022)]


# In[15]:


'Esta base tiene la variable de brillo solar promedio por estrato y por semana'

n_brillo_estra =Brillo_solar.groupby(['estrato','semana','año']).mean().reset_index()
n_brillo_estra.head()


# ### Calculos de las variables semanales

# In[16]:


#df_semanal
df_semanal = df_diario.groupby(['estrato','semana','año']).agg(
    temp_min_semanal=('temperatura_min_diaria', 'mean'),
    temp_max_semanal=('temperatura_max_diaria', 'mean'),
    temp_promedio_semanal=('temperatura_promedio_diaria', 'mean'),
    rad_diar_prom_semanal=('radiacion_diaria_promedio', 'mean'),
    ampl_term_prom_semanal = ('amplitud_termica', 'mean'),
    hume_rela_med_semanal= ('humedad_relativa_media_diaria','mean'),
    Veloc_viento_prom_max_semanal= ('velocidad_viento_max_diaria', 'mean'),
    Veloc_viento_media_semanal= ('velocidad_viento_media_diario', 'mean' ),
    max_lluvia=('lluvia_diaria', 'max'),
    acumu_lluvia=('lluvia_diaria', 'sum')

).reset_index().round(2).astype({'semana':int, 'año':int})


# ### Join 

# In[17]:


'Se cruza la base de datos de brillo solar semanal y la base con las demas variables semanales'
df_semanal=n_brillo_estra.merge(df_semanal,how='inner', on=['estrato','semana','año'])


# ### Base de datos precipitación

# In[18]:


'Se construye la base de datos para precipitación, en cuanto a la sumatoria agrupando por estacio, estrato, semana y año'


precip= df_diario.groupby(['estacion','estrato', 'semana','año']).agg(preci_semana=('lluvia_diaria', 'sum')).reset_index().astype({'semana':int, 'año':int})



'Despues de construir dicha base de precipitación se construye una base de datos del promedio de la precipitación clasificada pot estratos'

prec_prom_estra =precip.groupby(['estrato','semana','año']).mean().reset_index().round(2).astype({'semana':int, 'año':int})



prec_prom_estra["prec_acumulada"] = prec_prom_estra.groupby(['estrato','año'])["preci_semana"].cumsum().round(2)



prec_prom_estra[(prec_prom_estra.año==2022)&(prec_prom_estra.estrato=="Alto")]



'Se cruza la base de datos de precipitacion semanal y la base con las demas variables semanales'
df_semanal=prec_prom_estra.merge(df_semanal,how='inner', on=['estrato','semana','año'])

##### Ubicación de la tabla semanal
os.chdir(parametros['global']['pc'])



"""Se guarda la  base de datos con las variables calculadas semanalmente, en este caso se guarda en formato parquet dado
que este formato es mas liviano que descargar la bae en un excel, pero tambien es posible guardar en formato excel"""

df_semanal.to_parquet('semanal_complete.parquet')

#df_semanal.to_excel('semanal_complate.excel') 


# 
