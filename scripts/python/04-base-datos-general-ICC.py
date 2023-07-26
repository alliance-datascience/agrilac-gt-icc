#!/usr/bin/env python
# coding: utf-8

# # Base de datos diario -ICC
# 
# **Objetivo**
# Con este notebook se reproduce el excel que ICC maneja como insumo para sus calculos, reporte y demas.

# Libreías generales
import numpy as np
import pandas as pd
import math
import datetime
from dateutil.relativedelta import relativedelta
import glob
import datetime
import sklearn
import os
import yaml
import pyarrow  
import fastparquet 



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

# # features load
# los datos aqui cargados sufireron una limpieza en el notebook 
dfs = pd.read_parquet(parametros['global']['pc']+parametros['sateg4']['path_station_complete'])


# # Agregacion diaria

# In[5]:


def segmentacion(df, col_fecha):
    """
    función que segmenta la fecha en dia, mes, año y semana.
    
    Arguments:
        df: dataframe 
        col_fecha: columna que contiene la informacion de la fecha
        
    Returns:
        df: dataframe de entrada con 5 nuevas columnas que informan sobre laç
        fecha dd/mm/aa, semana, dia, mes y año
    """
    df['date'] = pd.to_datetime(df[col_fecha]).dt.date
    df['semana'] = pd.to_datetime(df[col_fecha]).dt.week
    df['dia'] = pd.to_datetime(df[col_fecha]).dt.day
    df['mes'] = pd.to_datetime(df[col_fecha]).dt.month
    df['año'] = pd.to_datetime(df[col_fecha]).dt.year
    return df


# In[6]:


#aqui se llama la funcion y se pone los parametros
segmentacion(dfs, 'fecha')


# # Agregaciones diarias
# con la siguiente linea se construyen las columnas agregadas que tenia carlos en el excel

# In[7]:


df_diario = dfs.groupby(['estacion', 'date','semana','dia','mes','año']).agg(
    temperatura_min_diaria=('temperatura', 'min'),
    temperatura_max_diaria=('temperatura', 'max'),
    temperatura_promedio_diaria=('temperatura', 'mean'),
    radiacion_diaria_acumulada=('radiacion', 'sum'),
    radiacion_diaria_promedio = ('radiacion','mean'),
    humedad_relativa_min_diaria = ('humedad_relativa', 'min'),
    humedad_relativa_max_diaria = ('humedad_relativa', 'max'),
    humedad_relativa_media_diaria = ('humedad_relativa', 'mean'),
    lluvia_diaria = ('precipitacion', 'sum'),
    velocidad_viento_media_diario = ('velocidad_viento', 'mean'),
    velocidad_viento_max_diaria = ('velocidad_viento', 'max')
).reset_index()


# In[8]:


df_diario['amplitud_termica'] = df_diario.temperatura_max_diaria - df_diario.temperatura_min_diaria


# # constantes para algunos calculos de variables
# las tablas que se consultan fueron sacadas del excel compartido por ICC. se esta adelantando la revision bibliografica para generalizar estas tablas a otras estaciones.

# In[9]:


"""
constantes de radiacion extraterreste y numero de dias despejado
"""

ra_n_ctes = pd.read_excel(parametros['global']['pc']+parametros['stage4']['path_tabla_Ra_N']).iloc[:-1]
ra_n_ctes.pop("Unnamed: 22")

ctes_radiacion = pd.read_excel(parametros['global']['pc']+parametros['stage4']['path_constantes_rad'])
ctes_radiacion.head()


# In[10]:


ctes_location = pd.read_excel(parametros['global']['pc']+parametros['stage4']['path_location'])
ctes_location['latitud_rad'] = ctes_location['latitud'].apply(lambda row: math.radians(row))


# In[11]:


def extraer_info(df, filtros_str, name_feature, name_col):
    """
    Como la tabla tiene la informacion de varias constantes
    de interes como por ejemplo: radiacion general y radiacion en dia p,
    se hace un filtro para separalas. como hacemos el filtro nos queda en las columnas los meses y en las filas con el dia.
    es necesario quitar  el sufijo lpara solo quedarnos con el numero de meses.

    la matriz anterior se re diseña para que el mes ya no quede como columna sino como filas
    y los valores de la radiacion no quede en la matriz sino en una columna
    
    
    Arguments:
        df: dataframe 
        col_fecha: columna que contiene la informacion de la fecha
        
    Returns:
        df: dataframe de entrada con 5 nuevas columnas que informan sobre laç
        fe

    """
    matriz=df.filter(regex=filtros_str)
    matriz.columns = matriz.columns.str.replace("_" + name_feature, "")
    df_feature = matriz.melt(id_vars=["dia"], 
        var_name="mes", 
        value_name=name_col)
    df_feature.mes = df_feature.mes.astype('int')
    return df_feature


# In[12]:


df_n = extraer_info(ra_n_ctes, 'dia|N', 'N', 'N Daylight hours')
df_ra = extraer_info(ra_n_ctes, 'dia|Ra', 'Ra', 'Ra')


# In[13]:


df_rg = extraer_info(ctes_radiacion, 'rgl|dia', 'rgl', 'radiacion_global')
df_rdp = extraer_info(ctes_radiacion, 'rdp|dia', 'rdp', 'radiacion_dia_despejado')


# ## Join all dfs

# In[14]:


"""
se unen los 3 dataframes donde las columnas comunes o las llaves son dia y mes y se 
reescribe el df_diario

"""
df_diario  = df_diario.merge(df_rg, how='inner', on=['dia', 'mes']).merge(df_rdp, how='inner', on=['dia', 'mes'])
df_diario = df_diario.merge(df_n, how='inner', on=['dia', 'mes']).merge(df_ra, how='inner', on=['dia', 'mes'])
##
df_diario = df_diario.merge(ctes_location, how= 'inner', on=['estacion'])


# ## Calculo de variables secundarias (las variables que requieren calculos)

# In[15]:


def secundary_variables(df):
    df['radiacion_media_estimada Heargreaves'] = 0.16 * (
        np.sqrt(df.amplitud_termica)
    ) * df.radiacion_global
    
    df['Rg'] = 0.00089681 * df.radiacion_diaria_acumulada
    
    df['n']=(
        -0.32 +1.61*(df.Rg / df.Ra)
    )*df['N Daylight hours']
    
    ## balance hidrico
    
    df['dia_juliano'] = df['date'].apply(
        lambda row: (row -datetime.date(row.year,1,1)).days +1
    )
    
    df['dr'] = 1 + (0.033 * np.cos(((2*3.1416)/365)*df['dia_juliano']))
    
    df['declinacion'] = 0.409  * np.sin(
        (((2*3.1416)/365)*df['dia_juliano'])-1.39
    )
    
    df['ang_horario_puesta_sol'] = np.arccos(
        -(np.tan(df.latitud_rad) * np.tan(df.declinacion))
    )
    
    df['heliofania'] = (24/3.1416) * df['ang_horario_puesta_sol']
    
    df['ra_MJm-2day-1'] = ((24*60)/3.1416) * (0.082*df['dr']) * (
        (
            (df.ang_horario_puesta_sol * np.sin(df.latitud_rad))*(np.sin(df.declinacion))
        )+(
            np.cos(df.latitud_rad)*np.cos(df.declinacion)*np.sin(df.ang_horario_puesta_sol)
        )
    )
    
    df['radiacion_solar_piranometro'] = 0.0864 * df.radiacion_diaria_promedio
    
    df['constante_psicometrica'] =(
        (
            pow(((293-(0.0065 * df.altitud))/293), 5.26)
        )*101.3
    )*0.000665
    
    df['pendiente_curva_presion_satu_vapor'] = (
        4098 * (
            (0.6108 * (
                np.exp((17.27*df.temperatura_promedio_diaria)/(df.temperatura_promedio_diaria+237.3))
            )) / pow(
                (df.temperatura_promedio_diaria+237.3),
                2)
        )
    )
    
    df['presion_saturacion_vapor'] = (
        (
            0.6108 * np.exp(
                (17.27*df.temperatura_min_diaria)/(df.temperatura_min_diaria +237.3)
            )
        )+(
            0.6108*np.exp(
                (17.27*df.temperatura_max_diaria)/(df.temperatura_max_diaria+237.3)
            )
        )
    )/2
    
    
    df['presion_real_vapor'] = (
        (
            0.6108*np.exp(
                (17.27*df.temperatura_max_diaria)/(df.temperatura_max_diaria +237.3)
            )
        ) * (df.humedad_relativa_min_diaria /100) + (
            0.6108*np.exp(
                (17.27*df.temperatura_min_diaria) / (df.temperatura_min_diaria+237.3)
            )
        ) * (df.humedad_relativa_max_diaria/100)
    )/2
    
    df['deficit_presion_vapor'] = df['presion_saturacion_vapor'] - df['presion_real_vapor']
    
    df['rns'] = df.radiacion_solar_piranometro * 0.77
    
    df['rnl']= (
        (
            (
                (
                    0.000000004903*(pow((df.temperatura_min_diaria+273.16),4))
                ) + (
                    (0.000000004903 * (pow(
                        (df.temperatura_max_diaria+273.16)
                        ,4)))
                )
            ) / 2
        ) * (
            0.34 -(0.14 *np.sqrt(df.presion_real_vapor))
        ) * (
            1.35 * (
                df.radiacion_solar_piranometro /((0.75+2*(df.altitud/100000))* df['ra_MJm-2day-1'])
            ) -0.35
        )
    )
    
    df['rn'] = df['rns'] - df['rnl']
    
    df['velocidad_viento_altura_standar'] = (
        (4.87)/((np.log((67.8*10)-5.42)))
    )*(
        (df.velocidad_viento_media_diario * 1000)/3600
    )
    
    df['FAO-Penman-Montieth'] = (
        (0.408 * df.pendiente_curva_presion_satu_vapor * df.rn) + (
            df.constante_psicometrica * (
                900/(df.temperatura_promedio_diaria + 273)
            )* df.velocidad_viento_altura_standar * df.deficit_presion_vapor
        )
    ) / (
        df.pendiente_curva_presion_satu_vapor + (
            df.constante_psicometrica * (
                1+(0.34 * df.velocidad_viento_altura_standar)
            )
        )
    )
    
    df['deficit']= df.lluvia_diaria - df['FAO-Penman-Montieth']
    return df



secundary_variables(df_diario)


##### Ubicación de la tabla general
os.chdir(parametros['global']['pc'])



df_diario.to_parquet('diario_complete.parquet')

