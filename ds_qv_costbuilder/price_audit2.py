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

query = """
match (n {id: 'E01.02.007'})
match (n)-[r:REQUIRES]->(m)
return n.id, n.total_price1, r.qty, r.uom, r.factor, r.waste, r.discount, r.margin, m.id, m.total_price1, m.total_price0, m.raw_price,
(r.qty * r.factor * r.waste * r.discount * r.margin * m.total_price0) as contribution
"""
query_df = graph.run(query).to_data_frame()
print(len(query_df))
print(query_df[['contribution']].sum())
