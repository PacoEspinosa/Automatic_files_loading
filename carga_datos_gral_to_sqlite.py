# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 23:30:09 2019

@author: fespinosa
"""

import sqlite3
import pandas
import os
import Complement_functions as cf

database_path = 'C:\\Users\\fespinosa\\Documents\\Trabajo\\DWH\\'
database_filename = 'staging.db'
conn = sqlite3.connect(database_path + database_filename)

filename_path = 'C:\\Users\\fespinosa\\Documents\\Trabajo\\tmp\\Carga\\'
filename_path_bck = 'C:\\Users\\fespinosa\\Documents\\Trabajo\\tmp\\Carga\\historico\\'

tables = ['act_status_tar','Cuentas_con_email','tmp_nombres','telefonos']
filesnames = ['act_status_tar','Cuentas_con_email','nombres1','telefonos']
extensions = ['.dat','.dat','.dat','.dat']
separators = ['|','\t','\t','\t']

for step in range(4):
    filepattern = filesnames[step]
    tablename = tables[step]
    separator = separators[step]
    files = []
    #print(step, filepattern, tablename)
    files = cf.listado_archivos(filename_path, filepattern)

    for filename in files:
        csvfile = filename_path  + filename
        bckfile = filename_path_bck  + filename
        try:
            #print(step, csvfile)
            df = pandas.read_table(csvfile, encoding='latin-1', header = 0, sep=separator, dtype = str)
            df.to_sql(tablename, conn, if_exists='replace', index=False)
            cursorObj = conn.cursor()
            #if tablename =='act_status_tar':
            #elif tablename =='Cuentas_con_email':
            #elif tablename =='tmp_nombres':
            #else:

            conn.commit()
            cursorObj.close()
            os.replace(csvfile, bckfile)
            
        except Exception as e:
                print('Error: {}'.format(str(e)) + ' Archivo:' + str(filename))    

conn.close()