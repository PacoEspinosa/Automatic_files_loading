# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 15:54:40 2020

@author: fespinosa
"""

import warnings

print('Before the warning')
warnings.warn('This is a warning message')
print('After the warning')

warnings.simplefilter('error', UserWarning)

print('Before the warning')
warnings.warn('This is a warning message')
print('After the warning')