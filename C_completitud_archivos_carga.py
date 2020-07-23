# -*- coding: utf-8 -*-
"""
Created on Wed Jul 22 13:09:21 2020

@author: fespinosa
"""
import os
import pandas as pd
import warnings

path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

warnings.simplefilter('ignore')

date1 = '2016-01-01'  
date2 = '2020-07-30'
filepattern = 'Hiscred_Oro'
datepattern = "%m%d%Y"
fileext = ".txt"

for r, d, f in os.walk(path):
    for file in f:
        files.append(file)

datelist = pd.date_range(date1,date2).tolist()

files_list = open(r"MyFile2.txt","w+") 
for date in datelist:
    filename = filepattern + date.strftime(datepattern) + fileext
    if filename in files:
        files_list.write('Proceso de carga terminado: ' + filename + "\r\n")
        print('Proceso de carga terminado: ' + filename)
    else:
        files_list.write('No se localizó el archivo: ' + filename + "\r\n")
        print('No se localizó el archivo: ' + filename)
        
files_list.close()