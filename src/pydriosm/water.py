# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 17:45:31 2022

@author: joe.chen
"""
from pyrosm import OSM, get_data
        
#%%

data_dir = "..//..//data//input//taiwan-latest.osm.pbf"
data = get_data("taiwan")

### 海岸線作業
### 1. 本島海岸線，海岸線每一百公里sep一段，取其中一段的way_id作為海岸線一百公里的ID(Rule1)
### 2. 離島要是發現有一些海岸線太短的，忽略他(Rule2)
### 3. 假設台灣的海岸線總共1411公里，理論上分成15段，最後一段11公里，但現在換成14段，最後一段特別延伸，不要讓最後一段特別短
