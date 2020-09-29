# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 13:53:35 2020

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
    SQL_text += ' where plan_apoyo is null and'
    SQL_text += " En_campaña in ('Plan')"
    SQL_text += " and tipo_matchl = 'Monto_justo'"
    SQL_text += ' group by num_credito;'

    cursor.execute(SQL_text)
    result = cursor_con.fetchall()
    for row in result:  
        SQL_text = 'insert into ' + final_table
        SQL_text += ' Select *'
        SQL_text += ' from ' + staging_table
        SQL_text += " where num_credito = '" + row[1] + "'" 
        SQL_text += " limit 5"
        SQL_text += ';'


    
