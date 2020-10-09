# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 13:53:35 2020

@author: fespinosa
"""

import os
import pymysql
import warnings
import Complement_functions as cf

#Variables
path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

warnings.simplefilter('ignore')

#funciones

    
#Constantes
filepattern = 'Reporte_P_Flexible_'
fileext = ".txt"
staging_table = 'Trabajos_prueba.tmp_transacciones_tnp_2020'
final_table = 'Trabajos_prueba.transacciones_tnp_2020'
pasos_proceso = 4
proceso = 'Carga reporte flexible'

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

try:
    paso = 0
    con = pymysql.connect(host = host, 
                      user = user, 
                      password = password, 
                      port = port,
                      autocommit=True,
                      local_infile=1)
    cursor = con.cursor()

    SQL_text = 'Select num_credito, count(*) as regs'
    SQL_text += ' from ' + staging_table
    SQL_text += ' where plan_apoyo is null'
    SQL_text += " and En_campaña in ('Plan')"
    SQL_text += " and tipo_matchl = 'Monto_justo'"
    SQL_text += ' group by num_credito'
    #SQL_text += ' having count(*) > 5'
    SQL_text += ' ;'

    SQL_text_2 = 'CREATE TABLE if not exists ' + final_table + ' ('
    SQL_text_2 += ' no_tarjeta char(16) DEFAULT NULL,'
    SQL_text_2 += ' monto double DEFAULT NULL,'
    SQL_text_2 += ' esnacional char(1) DEFAULT NULL,'
    SQL_text_2 += ' metodocaptura char(2) DEFAULT NULL,'
    SQL_text_2 += ' fechaoper char(11) DEFAULT NULL,'
    SQL_text_2 += ' codgironeg varchar(4) DEFAULT NULL,'
    SQL_text_2 += ' num_cliente varchar(9) DEFAULT NULL,'
    SQL_text_2 += ' num_credito varchar(13) DEFAULT NULL,'
    SQL_text_2 += ' folio_suc varchar(16) DEFAULT NULL,'
    SQL_text_2 += ' fecha_oper varchar(16) DEFAULT NULL,'
    SQL_text_2 += ' tipo_matchl varchar(25) DEFAULT NULL,'
    SQL_text_2 += ' En_campaña varchar(25) DEFAULT NULL,'
    SQL_text_2 += ' suc_origen varchar(4) DEFAULT NULL,'
    SQL_text_2 += ' plan_apoyo varchar(3) DEFAULT NULL,'
    SQL_text_2 += ' KEY tmp_idxtrxnstnp_tar (no_tarjeta),'
    SQL_text_2 += ' KEY tmp_idxtrxnstnp_cta (num_credito)'
    SQL_text_2 += ' ) ENGINE=InnoDB DEFAULT CHARSET=latin1;'
    cursor.execute(SQL_text_2)

    cursor.execute(SQL_text)
    result = cursor.fetchall()
#    n = 0    
    for row in result:  
        SQL_text = 'insert into ' + final_table
        SQL_text += ' Select *'
        SQL_text += ' from ' + staging_table
        SQL_text += " where num_credito = '" + str(row[0]) + "'"
        SQL_text += " and tipo_matchl = 'Monto_justo'"
        SQL_text += " limit 5"
        SQL_text += ';'
        cursor.execute(SQL_text)

except Exception as e:
    print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    

    
