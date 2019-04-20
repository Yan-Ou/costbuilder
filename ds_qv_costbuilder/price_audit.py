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

query = """match (n {id: 'E01.02.007'})
match p = (n)-[:REQUIRES*1..]->(m)-[:PRICE_IN]->(c:City {name: 'Auckland'})
return nodes(p) as q
"""
query_result = graph.run(query).data()
print(len(query_result))
print(query_result[0])
