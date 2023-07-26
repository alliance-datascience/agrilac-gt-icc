#!/usr/bin/env python
# coding: utf-8

# ## Lectura de Datos 
# 
# ##### Este notebook tiene como objetivo leer cada una de las hojas del excel de control de calidad de las estaciones meteorologicas de las que esta a cargo ICC

# In[1]:



## Libreías generales

import numpy as np
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import openpyxl
import os
import yaml

# Obtén la ruta del directorio actual (donde se encuentra script.py)
#directorio_actual = os.path.dirname(os.path.abspath(__file__))
#directorio_actual = getVariable("Internal.Transformation.Filename.Directory")

# Obtiene la ruta del archivo de script
ruta_script = os.path.realpath(__file__)

# Obtiene el directorio padre del archivo de script
directorio_actual = os.path.dirname(ruta_script)

# Construye la ruta al archivo YAML utilizando la ruta relativa
#ruta_yaml = os.path.join(directorio_actual, 'data.yaml')
#ruta_yaml = "D:/OneDrive - CGIAR/Proyectos/AgriLAC-ICC/ICC/Scrip_bases/Carpeta_compartir/data.yaml"

ruta_yaml = os.path.join(directorio_actual, 'data.yaml')



# Leer el archivo YAML
with open(ruta_yaml , 'r') as file:
    parametros= yaml.safe_load(file)

ruta_control = os.path.join(directorio_actual, parametros['stage1']['path_control'])
diccionario = pd.read_excel(ruta_control, sheet_name=None)


# Construir la ruta completa
path_control = parametros['global']['pc']+parametros['stage1']['path_control']


# Utilizar la ruta en tu código
datos=pd.DataFrame()
diccionario = pd.read_excel(path_control, sheet_name=None)





for nombre,columna in diccionario.items():
    columna['estacion']=nombre
    datos = pd.concat([datos, columna], ignore_index=True)
    

datos=datos[['estacion','Fecha', 'temperatura', 'radiacion', 'humedad relativa',
       'precipitacion', 'velocidad viento', 'mojadura', 'direccion viento','presion atmosferica']]



datos = datos.rename(columns={'Fecha':'fecha','velocidad viento':'velocidad_viento','humedad relativa':'humedad_relativa',
                                   'direccion viento':'direccion_viento', 'presion atmosferica': 'presion_atm'})



"""
Se crea un diccionario con las abreviaturas de los nombres de cada estación

"""

dict_esta= {'CEN':"CEN-CEN",'TEH':"LUT-TEH", 'BAL':"PAN-BAL", 'PUY':"TBU-PUY", 'SAV':"MAG-SAV", 'AMA':"SAA-AMA",
             'TRI':"SDT-TRI", 'IRL':"TBU-IRL", 'BON':"LUT-BON", 'BOU':"MAG-BOU", 'NRJ':"PAG-NRJ", 'PEO':"MAT-PEO",
             'CBR':"SDT-CBR", 'TLA': "TUL-TLA", 'LOR':"PAG-LOR" , 'SRF':"PAN-SRF", 'XOL':"MAG-XOL", 'SNC':"MAG-SNC",
             'TMG':"MAG-TMG", 'PLT':"ICC-PLT",  'LGR':"SAA-LGR", 'CDL':"ICC-CDL",  'CHI':"ICC-CHI", 'PVD':"MAG-PVD", 
             'ALA':"ICC-ALA", 'COC':"PAG-COC", 'CON':"ICC-CON", 'LMQ':"SAA-LMQ", 'STA':"ICC-STA", 'YEPO':"ICC-YEPO",
             'CIZ':"CASSA-CIZ",'CPL':"CASSA-CPL", 'CPT':"ASSA-CPT", 'ECA':"CASSA-ECA", 'SCL':"CASSA-SCL", 'AGS':"CASSA-AGS",
             'PBR':"ICC-PBR", 'MTA':"ICC-MTA", 'TUL': "TUL-TLA", 'NAR': "PAG-NRJ", 'LCD':"ICC-CDL", 'CASSA-CPT':"ASSA-CPT", 
             'SOTZ':"'ICC-SOTZ", 'JOY':"ICC-JOY", 'PUY':"TBU-PUY", 'CTB':"SDT-CBR" , 'SRAF':"PAN-SRF"
}


"""
Se realiza el reemplazo de las abrevituras a partir del diccionario creado

"""
datos.replace({"estacion": dict_esta},inplace=True)


###### Se crea la columna de tiempo
datos['Hora']=''
contador=0
for i in range(len(datos)):
    if contador==1440:
        contador=0
    Hora=int(contador/60)
    Minutos=contador%60
    Tiempo=str(Hora)+':'+ str(Minutos)
    datos.at[i,'Hora']=Tiempo
    contador+=15


# In[16]:


###### se transforma la columna fecha en caracter
datos['fecha']= datos.fecha.astype(str)
#### Se coloca en formato tiempo la columna Hora
datos['Hora']=pd.to_datetime(datos['Hora'], format='%H:%M')
#### Separo la hora de la fecha
datos['new_time'] = [d.time() for d in datos['Hora']]
##Elimino la columna ora
datos=datos.drop(['Hora'],axis=1)
#### la columna de tiempo la transformamos en caracter
datos['new_time']=datos.new_time.astype(str)
### En la columna del tiempo se toman los primero 5 cacateres que hacen referencia a la hora y minutos
datos["new_time"] = datos["new_time"].apply(lambda x: x[0:5])
### Se concatena la columna de fecha con el tiempo y se elimina la columna de tiempo, obteniendo así la data final
datos['fecha'] = datos.fecha.str.cat(datos.new_time, sep=' ')
datos=datos.drop(['new_time'],axis=1)


# In[24]:


def date_convert(date_to_convert):
     
     return datetime.datetime.strptime(date_to_convert, "%Y-%m-%d %H:%M").strftime("%Y-%d-%m %H:%M")


datos['fecha'] = datos['fecha'].apply(date_convert)


# In[25]:


datos.head()


# In[26]:


#### dirección donde se ubicaran los archivos de las estaciones
os.chdir(parametros['global']['pc']+parametros['stage1']['path_control'])



# In[27]:


### Guardar cada estacion es un archivo de excel diferente 

for i in datos.estacion.unique():
    
    df2=datos[datos['estacion']==i]
    df2.to_csv(f"{i}.csv",index=False, header=False)


# In[ ]:




