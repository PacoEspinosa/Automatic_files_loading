# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 12:57:41 2019

@author: fespinosa
"""

#import_xls_to_sqlite
import os
import warnings
import pandas
import Complement_functions as cf
from sqlalchemy import create_engine


#Variables
path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

warnings.simplefilter('ignore')

#Constantes
filepattern = 'Detalle de Sucursales Mayo 2021 (Inicio) Basico'
fileext = ".xlsx"
staging_table = 'tmp_cat_sucursales'
table = ''
historic_table = ''
pasos_proceso = 3
proceso = ''
tableName = 'Catalogos.cat_sucursales'
schema_name = 'Staging'

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

files = cf.listado_archivos(path, filepattern)

for filename in files:
    try:
        paso = 0
        sqlEngine       = create_engine('mysql+pymysql://' + user + ':' + password + '@' + host + '/' + schema_name, pool_recycle=3600)        
        dbConnection    = sqlEngine.connect()
        col_names = ['no_sucursal','nombre_sucursal','zona_banco','gerencia',
                     'nombre_gerencia','calle','num_ext','num_medio','num_int',
                     'colonia','municipio','estado','cp','estatus_dom',
                     'tel_sucursal']
        Sheets = ['Basico']
        for sheet in Sheets:
            #print(sheet)
            df = pandas.read_excel(filename, sheet_name=sheet, names=col_names)
            df.to_sql(staging_table, dbConnection, if_exists='replace', index=False);

    except ValueError as vx:
    
        print(vx)
    
    except Exception as ex:   
    
        print(ex)
    
    else:
    
        print("Table %s created successfully."%tableName);   
    
    finally:
    
        dbConnection.close()
            

if files == []:
    print('No se localizaron archivos de carga')
