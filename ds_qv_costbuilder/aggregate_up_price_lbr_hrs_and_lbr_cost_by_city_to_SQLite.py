# source activate qv
# 20181221 1154 JG New lbr_cost for Auckland for Martin's query
from py2neo import Graph, Node, Relationship
import os
import pandas as pd
import sqlite3
import sys

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
    print("\n" + "="*80)
    print("Processing {} {}".format(area, city))
    print("="*80)

    ####################################################################################

    print("Start aggregating up the levels")
    # Check status of current graph
    print("\nCheck")
    total_nodes = 0
    for type_of_node in ['Raw', 'Item', 'City', 'Bldg']:
        query = """MATCH (n:{}) RETURN count(n) as count""".format(type_of_node)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        total_nodes += count_of_nodes
        print("Count of {} nodes: {}".format(type_of_node, count_of_nodes))
    print("Total nodes: {}".format(total_nodes))

    ####################################################################################

    print("\n" + "="*80)
    print("Clear prices")
    # Clear any raw_price or total_price properties
    for type_of_price in ['raw', 'total', 'assigned']:
        query = """MATCH (n) WHERE EXISTS(n.{type_of_price}_price) REMOVE n.{type_of_price}_price""".format(type_of_price=type_of_price)
        query_result = graph.run(query)

    # Check count of prices of current graph
    print("\nCheck")
    for type_of_price in ['raw', 'total', 'assigned']:
        query = """MATCH (n) WHERE EXISTS(n.{type_of_price}_price) RETURN count(n) as count""".format(type_of_price=type_of_price)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        print("Count of nodes with {}_price: {}".format(type_of_price, count_of_nodes))

    ####################################################################################

    print("\n" + "="*80)
    print("Clear lbr_hrs")
    query = """MATCH (n) WHERE EXISTS(n.lbr_hrs) REMOVE n.lbr_hrs"""
    query_result = graph.run(query)

    # Check count of lbr_hrs of current graph
    print("\nCheck")
    query = """MATCH (n) WHERE EXISTS(n.lbr_hrs) RETURN count(n) as count"""

    query_df = graph.run(query).to_data_frame()
    count_of_nodes = query_df['count'][0]
    print("Count of nodes with lbr_hrs: {}".format(count_of_nodes))

    ####################################################################################

    print("\n" + "="*80)
    print("Clear levels")
    # Clear any level property
    query = """MATCH (n) WHERE EXISTS(n.level) REMOVE n.level"""
    query_result = graph.run(query)

    # Check count of levels
    print("\nCheck")
    for type_of_node in ['Raw', 'Item', 'City', 'Bldg']:
        query = """MATCH (n:{}) WHERE EXISTS(n.level) RETURN count(n) as count""".format(type_of_node)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        print("Count of {} nodes with level: {}".format(type_of_node, count_of_nodes))

    print("\n" + "="*80)
    print("Set levels\n")
    # STEP 0 find the longest path of requirements for each node
    query = """match (n)
    match p = (n)-[:REQUIRES*1..]->(m)
    WITH n, max(length(p)) as L
    SET n.level = L
    return count(n) as count"""

    query_df = graph.run(query).to_data_frame()
    count_of_nodes = query_df['count'][0]
    print("Count of nodes with level added: {}".format(count_of_nodes))

    # Check max level
    query = """match (n)
    return max(n.level) as max_level"""

    query_df = graph.run(query).to_data_frame()
    max_level = query_df['max_level'][0]
    print("MAX level: {}".format(max_level))

    # Check count of levels
    print("\nCheck")
    for type_of_node in ['Raw', 'Item', 'City', 'Bldg']:
        query = """MATCH (n:{}) WHERE EXISTS(n.level) RETURN count(n) as count""".format(type_of_node)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        print("Count of {} nodes with level: {}".format(type_of_node, count_of_nodes))

    total_nodes = 0
    for type_of_node in ['Item', 'Bldg']:
        print("\nCheck")
        for level in range(1, max_level+1):
            query = """MATCH (n:{}) WHERE n.level={} RETURN count(n) as count""".format(type_of_node, level)

            query_df = graph.run(query).to_data_frame()
            count_of_nodes = query_df['count'][0]
            total_nodes += count_of_nodes
            print("Count of {} nodes with level of {}: {}".format(type_of_node, level, count_of_nodes))
    print("Total {} nodes with levels: {}".format(type_of_node, total_nodes))    

    ####################################################################################

    print("\n" + "="*80)
    print("Set prices\n")

    # STEP 0 Set assigned_prices AS raw_prices (can be overwritten in next step)
    type_of_price = 'assigned'
    query = """MATCH (i:Item)-[price_in:PRICE_IN]->(j:Assign)
    WITH i, price_in.price AS raw_price
    SET i.assigned_price = raw_price
    SET i.raw_price = raw_price
    RETURN count(i) as count"""

    query_df = graph.run(query).to_data_frame()
    count_of_nodes = query_df['count'][0]
    print("Count of nodes with {}_price (also raw): {}".format(type_of_price, count_of_nodes))

    # STEP 1 Set raw_prices
    type_of_price = 'raw'
    query = """MATCH (i:Item)-[r:REQUIRES]->(j:Raw)-[price_in:PRICE_IN]->(:City {{name: '{}'}})
    WITH i, sum(r.qty * r.discount * r.factor * r.margin * r.waste * price_in.price) AS raw_price, 
    sum(r.qty * r.factor * price_in.price * coalesce(r.include_in_lbr_hrs,0)) AS lbr_cost, 
    sum(r.qty * r.factor* coalesce(r.include_in_lbr_hrs,0)) as lbr_hrs
    SET i.raw_price = raw_price
    SET i.lbr_cost = lbr_cost
    SET i.lbr_hrs = lbr_hrs
    RETURN count(i) as count""".format(city)    

    query_df = graph.run(query).to_data_frame()
    count_of_nodes = query_df['count'][0]
    print("Count of nodes with {}_price: {}".format(type_of_price, count_of_nodes))

    # STEP 2 Some nodes will have BOTH a raw_price and, because they use other items as well, a total_price
    type_of_price = 'total'
    query = """MATCH (i:Item)-[s:REQUIRES]->(j:Item)
    WHERE EXISTS(i.raw_price) AND EXISTS(j.raw_price)
    WITH i, i.raw_price + sum(s.qty * s.discount * s.factor * s.margin * s.waste * j.raw_price) as total_price, 
    coalesce(i.lbr_cost,0) + sum(s.qty * s.factor * coalesce(j.lbr_cost,0)) as lbr_cost,
    coalesce(i.lbr_hrs,0) + sum(s.qty * s.factor * coalesce(j.lbr_hrs,0)) as lbr_hrs
    SET i.total_price = total_price
    SET i.lbr_cost = lbr_cost
    SET i.lbr_hrs = lbr_hrs
    RETURN count(i) as count"""

    query_df = graph.run(query).to_data_frame()
    count_of_nodes = query_df['count'][0]
    print("Count of nodes with {}_price: {}".format(type_of_price, count_of_nodes))

    # STEP 3 Now we need to fill in the blanks for total price
    type_of_price = 'total'
    query = """MATCH (i:Item)
    WHERE NOT EXISTS(i.total_price) AND EXISTS(i.raw_price)
    SET i.total_price = i.raw_price
    RETURN count(i) as count"""

    query_df = graph.run(query).to_data_frame()
    count_of_nodes = query_df['count'][0]
    print("Count of nodes with {}_price added AS raw_price: {}".format(type_of_price, count_of_nodes))

    # Check current status of graph
    print("\nCheck")
    total_nodes = 0
    for type_of_node in ['Raw', 'Item', 'City', 'Bldg']:
        query = """MATCH (n:{}) WHERE EXISTS(n.total_price) RETURN count(n) as count""".format(type_of_node)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        total_nodes += count_of_nodes
        print("Count of {} nodes with total_price: {}".format(type_of_node, count_of_nodes))
    print("Total nodes with total_price: {}".format(total_nodes))

    print("\nCheck")
    total_nodes = 0
    for level in range(1, max_level+1):
        query = """MATCH (n:Item) WHERE n.level={} AND EXISTS(n.total_price) RETURN count(n) as count""".format(level)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        total_nodes += count_of_nodes
        print("Count of nodes with total_price and level of {}: {}".format(level, count_of_nodes))
    print("Total nodes with total_price and levels: {}".format(total_nodes))

    print("\nCheck")
    total_nodes = 0
    for level in range(1, max_level+1):
        query = """MATCH (n:Item) WHERE n.level={} AND EXISTS(n.lbr_cost) RETURN count(n) as count""".format(level)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        total_nodes += count_of_nodes
        print("Count of nodes with lbr_cost and level of {}: {}".format(level, count_of_nodes))
    print("Total nodes with lbr_cost and levels: {}".format(total_nodes))


    # STEP 4 Now we can fill in the next levels of required total prices
    # NOTE ALL j must have total price
    for level in range(1, max_level+1):
        print("\nAdd level {}".format(level))
        type_of_price = 'total'
        query = """MATCH (i)-[s:REQUIRES]->(j)
        WHERE i.level = {} AND NOT EXISTS(i.total_price)
        WITH i, sum(s.qty * s.discount * s.factor * s.margin * s.waste * j.total_price) as total_price, 
        coalesce(i.lbr_cost,0) + sum(s.qty * s.factor * coalesce(j.lbr_cost,0)) as lbr_cost,
        coalesce(i.lbr_hrs,0) + sum(s.qty * s.factor * coalesce(j.lbr_hrs,0)) as lbr_hrs
        SET i.total_price = total_price
        SET i.lbr_cost = lbr_cost
        SET i.lbr_hrs = lbr_hrs
        RETURN count(i) as count""".format(level)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        print("Count of nodes with {}_price added at level {}: {}".format(type_of_price, level, count_of_nodes))


    print("\nCheck")
    total_nodes = 0
    for level in range(1, max_level+1):
        query = """MATCH (n) WHERE n.level={} AND EXISTS(n.total_price) RETURN count(n) as count""".format(level)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        total_nodes += count_of_nodes
        print("Count of nodes with total_price and level of {}: {}".format(level, count_of_nodes))
    print("Total nodes with total_price and levels: {}".format(total_nodes))

    print("\nCheck")
    total_nodes = 0
    for level in range(1, max_level+1):
        query = """MATCH (n:Item) WHERE n.level={} AND EXISTS(n.lbr_cost) RETURN count(n) as count""".format(level)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        total_nodes += count_of_nodes
        print("Count of nodes with lbr_cost and level of {}: {}".format(level, count_of_nodes))
    print("Total nodes with lbr_cost and levels: {}".format(total_nodes))


    print("Finished aggregating up the levels")

    if area == 'AREA1':
        query = """MATCH (n) WHERE EXISTS(n.total_price) return n.id as id, n.total_price as {}, n.lbr_cost as lbr_cost, n.lbr_hrs as lbr_hrs""".format(area)
    else:
        query = """MATCH (n) WHERE EXISTS(n.total_price) return n.id as id, n.total_price as {}""".format(area)

    query_df = graph.run(query).to_data_frame()
    print("Count of nodes with total_price in {}: {}".format(city, len(query_df)))
    if area == 'AREA1':
        all_df = query_df[['id','AREA1','lbr_cost','lbr_hrs']].copy()
    else:
        all_df = all_df.merge(query_df[['id', area]], how="outer", on="id")
        print(list(all_df.columns))

columns_to_output = ["id","AREA1","AREA2","AREA3","AREA4","AREA5","AREA6","lbr_hrs","lbr_cost"]
print("Output only {}".format(columns_to_output))
all_df.sort_values(by='id', inplace=True)
all_df[columns_to_output].to_csv('qv_costbuilder.csv', index=False)
all_df[columns_to_output].to_sql('qv_costbuilder', connection, index=False, if_exists='replace')
print("Created SQLite table qv_costbuilder and qv_costbuilder.csv")

"""
(qv) John-Graves-Qrious:qv johngraves$ python "20181218 1623 price by city saved in SQLite.py" > price_by_city.log
See .log
"""









