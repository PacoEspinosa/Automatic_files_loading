# -*- coding: utf-8 -*-
"""
Created on Thu May 14 00:04:21 2020

@author: fespinosa
"""

import warnings
import pymysql

warnings.simplefilter('ignore')

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

date_ant = '2020-05'
datelist = ['2020-06','2020-07']

for date in datelist:
    anio_ant = date_ant[0:4]
    mes_ant = date_ant[-2:]
    anio = date[0:4]
    mes = date[-2:]    


    con = pymysql.connect(host = host, 
                      user = user, 
                      password = password, 
                      port = port,
                      autocommit=True,
                      local_infile=1)
    cursor = con.cursor()

    sql_text = "drop table if exists Trabajos_prueba.pagos_minimos;"
    cursor.execute(sql_text)

    sql_text = "create table Trabajos_prueba.pagos_minimos  as select num_credito, pago_min_total, saldos, limite_credito"
    sql_text += " from Historicos.Financieros"
    sql_text += " where pago_min_total > 0"
    sql_text += " and substr(fecha,1,4) = '" + anio_ant + "'"
    sql_text += " and substr(fecha,6,2) = '" + mes_ant + "';"
    cursor.execute(sql_text)

    sql_text = "create index tmp_idx_pagmin on Trabajos_prueba.pagos_minimos (num_credito asc);"
    cursor.execute(sql_text)

    sql_text = "drop table if exists Trabajos_prueba.pagos_totales;"
    cursor.execute(sql_text)

    sql_text = "create table Trabajos_prueba.pagos_totales as select año, mes, num_credito, mto_pagos"
    sql_text += " from Cuentas_tc.cuentas_trancred"
    sql_text += " where año = '" + anio + "' and mes = '" + mes + "';"
    cursor.execute(sql_text)

    sql_text = "create index tmp_idx_pagtot on Trabajos_prueba.pagos_totales (num_credito asc);"
    cursor.execute(sql_text)

    sql_text = "insert into Analisis.Pagos_excedentes select '" + anio + "' 'año', '" + mes + "' 'mes',"
    sql_text += " case when mto_pagos is null then 'Rango vencido'"
    sql_text += " when mto_pagos = 0 then 'Rango 0'"
    sql_text += " when mto_pagos < pago_min_total then 'Rango 1'"
    sql_text += " when floor(mto_pagos/pago_min_total) <= 1 then 'Rango 2'"
    sql_text += " when floor(mto_pagos/pago_min_total) <= 5 then 'Rango 3'"
    sql_text += " when floor(mto_pagos/pago_min_total) <= 10 then 'Rango 4' else 'Rango 5' end as pago_excedente,"
    sql_text += " sum(saldos) as vol_saldo, sum(mto_pagos) as vol_pagos, sum(limite_credito) as exposicion,"
    sql_text += " count(*) as ctas"
    sql_text += " from Trabajos_prueba.pagos_minimos a left join Trabajos_prueba.pagos_totales b"
    sql_text += " on a.num_credito = b.num_credito"
    sql_text += " group by  año, mes,"
    sql_text += " case when ifnull(mto_pagos,0) = 0 then 'Rango 0'"
    sql_text += " when mto_pagos < pago_min_total then 'Rango 1'"
    sql_text += " when floor(mto_pagos/pago_min_total) <= 1 then 'Rango 2'"
    sql_text += " when floor(mto_pagos/pago_min_total) <= 5 then 'Rango 3'"
    sql_text += " when floor(mto_pagos/pago_min_total) <= 10 then 'Rango 4'"
    sql_text += " else 'Rango 5' end;"
    cursor.execute(sql_text)

    print('Proceso terminado: ' + date)

    date_ant = date

con.close()

 
