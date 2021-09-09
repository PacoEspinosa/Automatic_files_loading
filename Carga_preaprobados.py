# -*- coding: utf-8 -*-
"""
Created on Fri Aug 27 18:15:47 2021

@author: fespinosa
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
filepattern = 'ETIQUETASEGMENTOS_'
fileext = ".txt"
staging_table = 'Staging.preaprobados'
table = 'Cuentas_tc.Preaprobados'
historic_table = 'Historicos.Preaprobados'
pasos_proceso = 3
proceso = 'Carga clientes preaprobados riesgos'
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
            load_sql = "load data local infile '" + filename + "' into table " + staging_table
            load_sql += " fields terminated by '|'"
            load_sql += " lines terminated by '\r\n'"
            load_sql += " ignore 1 lines"
            load_sql += ";"
            #print(filename)
            cursor.execute('truncate table ' + staging_table + ';')
            cursor.execute(load_sql)
            #cf.logging_carga(cursor, filename, staging_table)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo solicitudes')

            paso = 2
            staging_step_2a = "truncate " + table
            cursor.execute(staging_step_2a)
            staging_step_2b = "insert ignore into " + table + " ("
            staging_step_2b += " num_cliente,"
            staging_step_2b += " cosecha,"
            staging_step_2b += " cap_meses,"
            staging_step_2b += " cap_promedio6M,"
            staging_step_2b += " cap_depositos_tot6M,"
            staging_step_2b += " sucursal_cliente,"
            staging_step_2b += " cap_cuentas,"
            staging_step_2b += " inversion,"
            staging_step_2b += " cap_remesas_tot6M,"
            staging_step_2b += " remesas,"
            staging_step_2b += " cap_mesesAnt,"
            staging_step_2b += " segmento,"
            staging_step_2b += " cliente_clase"
            staging_step_2b += " ) "
            staging_step_2b += " select "
            staging_step_2b += " num_cliente,"
            staging_step_2b += " cosecha,"
            staging_step_2b += " cap_meses,"
            staging_step_2b += " cap_promedio6M,"
            staging_step_2b += " cap_depositos_tot6M,"
            staging_step_2b += " sucursal_cliente,"
            staging_step_2b += " cap_cuentas,"
            staging_step_2b += " inversion,"
            staging_step_2b += " cap_remesas_tot6M,"
            staging_step_2b += " remesas,"
            staging_step_2b += " cap_mesesAnt,"
            staging_step_2b += " segmento,"
            staging_step_2b += " cliente_clase"
            staging_step_2b += " from " + staging_table
            cursor.execute(staging_step_2b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta registros en tabla del mes')
            
            paso = 3
            staging_step_3 = "insert ignore into " + historic_table + " select "
            staging_step_3 += " num_cliente,"
            staging_step_3 += " cosecha,"
            staging_step_3 += " cap_meses,"
            staging_step_3 += " cap_promedio6M,"
            staging_step_3 += " cap_depositos_tot6M,"
            staging_step_3 += " cap_cuentas,"
            staging_step_3 += " inversion,"
            staging_step_3 += " cap_remesas_tot6M,"
            staging_step_3 += " remesas,"
            staging_step_3 += " cap_mesesAnt,"
            staging_step_3 += " segmento,"
            staging_step_3 += " cliente_clase"
            staging_step_3 += " from " + staging_table
            cursor.execute(staging_step_3)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta registros en tabla historica')
            
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    

if files == []:
    print('No se localizaron archivos de carga')
