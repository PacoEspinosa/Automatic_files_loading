#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 23:34:44 2019
subject: Proceso de carga historica, donde se conoce las fechas disponibles
 y los archivos a cargar se encuentran en una carpeta especifica.
@author: francisco
"""

import os
import pymysql
import warnings
import datetime
import Complement_functions as cf

#Variables
path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

warnings.simplefilter('ignore')

#funciones

    
#Constantes
filepattern = 'Largos_Dic20'
fileext = ".csv"
staging_table = 'tmp_ctes_largos2'
table = 'Cuentas_tc.Clientes_largos2'
historic_table = 'Historicos.Clientes_largos2'
pasos_proceso = 3
proceso = 'Carga clientes largos riesgos'
fec_seguimiento = datetime.datetime.today().strftime("%Y-%m")

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

files = cf.listado_archivos(path, filepattern)

#filename = filepattern + '05' + datetime.datetime.today().strftime("%m%Y") + fileext
for filename in files:
    try:
        paso = 0
        con = pymysql.connect(host = host, 
                          user = user, 
                          password = password, 
                          port = port,
                          autocommit=True,
                          local_infile=1)
        cursor = con.cursor()
        if cf.Validacion_archivo(cursor, filename):
            print('Archivo previamente cargado: ' + filename)
            con.close()
        else:
            paso = 1
            load_sql = "load data local infile '" + filename + "' into table Staging." + staging_table
            load_sql += " fields terminated by ','"
            load_sql += " optionally enclosed by '" + chr(34) + "'"
            load_sql += " lines terminated by '\r\n'"
            load_sql += " ignore 1 lines"
            load_sql += ";"
            #print(filename)
            cursor.execute('truncate table Staging.' + staging_table + ';')
            cursor.execute(load_sql)
            cf.logging_carga(cursor, filename, staging_table)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo solicitudes')

            paso = 2
            staging_step_2a = "truncate " + table
            cursor.execute(staging_step_2a)
            staging_step_2b = "insert into " + table + " ("
            staging_step_2b += " sucursal_cliente,"
            staging_step_2b += " num_cliente,"
            staging_step_2b += " fecha_proceso,"
            staging_step_2b += " fecha_apertura,"
            staging_step_2b += " segmento,"
            staging_step_2b += " cliente_clase,"
            staging_step_2b += " edo_sucursal,"
            staging_step_2b += " mpo_sucursal,"
            staging_step_2b += " cp_sucursal,"
            staging_step_2b += " nombre_sucursal"
            staging_step_2b += " ) "
            staging_step_2b += " select "
            staging_step_2b += " sucursal_cliente,"
            staging_step_2b += " num_cliente,"
            staging_step_2b += " fecha_proceso,"
            staging_step_2b += " fecha_apertura,"
            staging_step_2b += " segmento,"
            staging_step_2b += " cliente_clase,"
            staging_step_2b += " edo_sucursal,"
            staging_step_2b += " mpo_sucursal,"
            staging_step_2b += " cp_sucursal,"
            staging_step_2b += " nombre_sucursal"
            staging_step_2b += " from Staging." + staging_table
            cursor.execute(staging_step_2b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta registros en tabla del mes')
            """
            paso = 3
            staging_step_3 = "insert into " + historic_table + " select '" + fec_seguimiento + "' ,"
            staging_step_3 += " num_cliente,"
            staging_step_3 += " num_cuenta,"
            staging_step_3 += " nombre1,"
            staging_step_3 += " nombre2,"
            staging_step_3 += " apell_paterno,"
            staging_step_3 += " apell_materno,"
            staging_step_3 += " correo_elec,"
            staging_step_3 += " codpostal,"
            staging_step_3 += " estado,"
            staging_step_3 += " casa,"
            staging_step_3 += " celular,"
            staging_step_3 += " saldo"
            staging_step_3 += " from Staging." + staging_table
            cursor.execute(staging_step_3)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta registros en tabla historica')
            """   
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    

if files == []:
    print('No se localizaron archivos de carga')
