# source activate qv
from py2neo import Graph, Node, Relationship
import os
import pandas as pd
import sqlite3
import sys
from aggregate_up_the_levels import *

BASE_PATH = '.'

# Database Credentials
uri             = "bolt://localhost:7687"
userName        = "neo4j"
password        = "qrious@2018"

graph = Graph(uri, auth=(userName, password))

# Get list of cities
query = """MATCH (i:City) RETURN i.name as name"""

query_df = graph.run(query).to_data_frame()
cities = list(query_df['name'])
print("Processing price database for cities:")
areas = []
for i in range(len(cities)):
    areas.append("AREA{}".format(i+1))
_ = [print(item[0], item[1]) for item in zip(areas, cities)]

# Delete qv_costbuilder table
connection = sqlite3.connect(os.path.join(BASE_PATH, "qv_costbuilder.db"))
cursor = connection.cursor()
print("\nDROP qv_costbuilder table")
query = "DROP TABLE IF EXISTS qv_costbuilder"
cursor.execute(query)

for area, city in zip(areas, cities):
    aggregate_up_the_levels(city=city, area=area, graph=graph)

    if area == 'AREA1':
        query = """MATCH (n) WHERE EXISTS(n.total_price) return n.id as id, n.total_price as {}, 
        n.total_material_cost as total_material_cost_AREA1, 
        n.total_plant_cost as total_plant_cost_AREA1, 
        n.total_labour_cost as total_labour_cost_AREA1, n.lbr_hrs as lbr_hrs""".format(area)
    else:
        query = """MATCH (n) WHERE EXISTS(n.total_price) return n.id as id, n.total_price as {}""".format(area)

    query_df = graph.run(query).to_data_frame()
    print("Count of nodes with total_price in {}: {}".format(city, len(query_df)))
    if area == 'AREA1':
        all_df = query_df[['id','AREA1',"total_material_cost_AREA1", "total_plant_cost_AREA1", "total_labour_cost_AREA1",'lbr_hrs']].copy()
    else:
        all_df = all_df.merge(query_df[['id', area]], how="outer", on="id")
        print(list(all_df.columns))

columns_to_output = ["id","AREA1","AREA2","AREA3","AREA4","AREA5","AREA6","lbr_hrs","total_material_cost_AREA1", "total_plant_cost_AREA1", "total_labour_cost_AREA1"]
print("Output only {}".format(columns_to_output))
all_df.sort_values(by='id', inplace=True)
all_df[columns_to_output].to_csv('qv_costbuilder.csv', index=False)
all_df[columns_to_output].to_sql('qv_costbuilder', connection, index=False, if_exists='replace')
print("Created SQLite table qv_costbuilder and qv_costbuilder.csv")

"""
(qv) John-Graves-Qrious:qv johngraves$ python "20181218 1623 price by city saved in SQLite.py" > price_by_city.log
See .log
"""









