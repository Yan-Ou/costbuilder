# source activate qv
from py2neo import Graph, Node, Relationship
import os
import pandas as pd
import sqlite3
import sys

# Database Credentials
uri             = "bolt://localhost:7687"
userName        = "neo4j"
password        = "qrious@2018"

graph = Graph(uri, auth=(userName, password))

# Check status of current graph
print("\nCheck (n)")
total_nodes = 0
for type_of_node in ['Raw', 'Item', 'City', 'Bldg']:
    query = """MATCH (n:{}) RETURN count(n) as count""".format(type_of_node)

    query_df = graph.run(query).to_data_frame()
    count_of_nodes = query_df['count'][0]
    total_nodes += count_of_nodes
    print("Count of {} nodes: {}".format(type_of_node, count_of_nodes))
print("Total nodes: {}".format(total_nodes))

print("="*80)
print("Outgoing")
for relationship in ['PRICE_IN', 'REQUIRES']:
    print("\nOutgoing {}".format(relationship))
    for has_or_not in ['', 'NOT']:
        print("\nCheck {} :{} (n)-[]->()".format(has_or_not, relationship))
        total_nodes = 0
        for type_of_node in ['Raw', 'Item', 'City', 'Bldg']:
            query = """MATCH (n:{}) WHERE {} (n)-[:{}]->() RETURN count(n) as count """.format(type_of_node, has_or_not, relationship)

            query_df = graph.run(query).to_data_frame()
            count_of_nodes = query_df['count'][0]
            total_nodes += count_of_nodes
            print("Count of {} nodes with {} :{} outgoing: {}".format(type_of_node, has_or_not, relationship, count_of_nodes))
        print("Total nodes with {} :{} outgoing: {}".format(total_nodes, has_or_not, relationship))

print("="*80)
print("Incoming")
for relationship in ['PRICE_IN', 'REQUIRES']:
    print("\nIncoming {}".format(relationship))
    for has_or_not in ['', 'NOT']:
        print("\nCheck {} :{} ()-[]->(n)".format(has_or_not, relationship))
        total_nodes = 0
        for type_of_node in ['Raw', 'Item', 'City', 'Bldg']:
            query = """MATCH (n:{}) WHERE {} ()-[:{}]->(n) RETURN count(n) as count """.format(type_of_node, has_or_not, relationship)

            query_df = graph.run(query).to_data_frame()
            count_of_nodes = query_df['count'][0]
            total_nodes += count_of_nodes
            print("Count of {} nodes with {} :{} incoming: {}".format(type_of_node, has_or_not, relationship, count_of_nodes))
        print("Total nodes with {} :{} incoming: {}".format(total_nodes, has_or_not, relationship))

print("="*80)
print("Between")
for relationship in ['PRICE_IN', 'REQUIRES']:
    print("\nBetween {}".format(relationship))
    for has_or_not in ['', 'NOT']:
        print("\nCheck {} :{} (n)-[]->() AND ()-[]->(n)".format(has_or_not, relationship))
        total_nodes = 0
        for type_of_node in ['Raw', 'Item', 'City', 'Bldg']:
            query = """MATCH (n:{type_of_node}) WHERE {has_or_not} (n)-[:{relationship}]->() AND {has_or_not} ()-[:{relationship}]->(n) RETURN count(n) as count """.format(type_of_node=type_of_node, 
                has_or_not=has_or_not, 
                relationship=relationship)

            query_df = graph.run(query).to_data_frame()
            count_of_nodes = query_df['count'][0]
            total_nodes += count_of_nodes
            print("Count of {} nodes with {} :{} between: {}".format(type_of_node, has_or_not, relationship, count_of_nodes))
        print("Total nodes with {} :{} between: {}".format(total_nodes, has_or_not, relationship))

print("="*80)
print("total_price")
for has_or_not in ['', 'NOT']:
    print("\nCheck {} EXISTS(n.total_price)".format(has_or_not))
    total_nodes = 0
    for type_of_node in ['Raw', 'Item', 'City', 'Bldg']:
        query = """MATCH (n:{type_of_node}) WHERE {has_or_not} EXISTS(n.total_price) RETURN count(n) as count """.format(type_of_node=type_of_node, 
            has_or_not=has_or_not)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        total_nodes += count_of_nodes
        print("Count of {} nodes with {} total_price: {}".format(type_of_node, has_or_not, count_of_nodes))
    print("Total nodes with {} total_price: {}".format(total_nodes, has_or_not))
