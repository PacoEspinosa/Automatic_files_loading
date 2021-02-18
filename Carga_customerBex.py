# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 12:57:41 2019

@author: fespinosa
"""

#import_xls_to_sqlite
import os
import pymysql
import warnings
import pandas
import Complement_functions as cf
from sqlalchemy import create_engine


#Variables
path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

warnings.simplefilter('ignore')

#Constantes
filepattern = 'CustomerBEX2.0'
fileext = ".xlsx"
staging_table = 'tmp_ctes_largos2'
table = 'Cuentas_tc.Clientes_largos2'
historic_table = 'Historicos.Clientes_largos2'
pasos_proceso = 3
proceso = 'Carga clientes largos riesgos'
tableName = 'CustomerBX'

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
        sqlEngine       = create_engine('mysql+pymysql://' + user + ':' + password + '@' + host + '/Cuentas_tc', pool_recycle=3600)        
        dbConnection    = sqlEngine.connect()
        col_names = ['_id','customerNumber','cellphone']
        Sheets = ['CustomerBEX2']
        for sheet in Sheets:
            #print(sheet)
            df = pandas.read_excel(filename, sheet_name=sheet)
            df.to_sql(tableName, dbConnection, if_exists='append', index=False);

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
