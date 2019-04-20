# source activate qv
# 20190117 1424 Use aggregate_up_the_levels function
# 20190117 1002 Add Plant costs roll-up like labour cost roll-up
# 20181218 1134 Sum up lbr_hrs
# 20181218 1100 Switch from MERGE to CREATE relationships to cover case of 2 PilingCrew in T05.07.081 in 2Prep_1
# 20181212 1359 Error when attempting to MATCH to node not on graph
# 20181212 1128 Outputs .csv files like 7Finishes_1_needs_correction.csv if Excel has formulas NOT IN formula_lookup.xlsx
# 20181210 1611 Includes Assigned prices for some items
from py2neo import Graph, Node, Relationship
import os
import pandas as pd
import sqlite3
import sys

from aggregate_up_the_levels import *

BASE_PATH = '.'

USE_SQL = True
print("USE_SQL is {}".format(USE_SQL))

AGGREGATE_UP_AUCKLAND = True
print("AGGREGATE_UP_AUCKLAND is {}".format(AGGREGATE_UP_AUCKLAND))

INPUTS_PATH = os.path.join(BASE_PATH, 'costbuilder_inputs') # lookups

PATH_TO_WORKBOOKS_XLSX = os.path.join(INPUTS_PATH, 'workbooks') # 1 House B1, breakdown.xlsx ...
PATH_TO_BLOCKS_XLSX = os.path.join(INPUTS_PATH, 'blocks') # 2Prep_1.xlsx ...
PATH_TO_PRICES_XLSX = os.path.join(INPUTS_PATH, 'prices') # 
PATH_TO_ASSIGNED_XLSX = os.path.join(INPUTS_PATH, 'assigned') # assigned.xlsx 
PATH_TO_PRICED_ELEMENTS_XLSX = os.path.join(INPUTS_PATH, 'priced_elements') # priced_elements.xlsx 

PATH_TO_AUDIT = os.path.join(INPUTS_PATH, 'audit')
if not os.path.exists(PATH_TO_AUDIT):
    os.mkdir(PATH_TO_AUDIT)

# Database Credentials
uri             = "bolt://localhost:7687"
userName        = "neo4j"
password        = "qrious@2018"

try:
    print("Connecting to Neo4j")
    graph = Graph(uri, auth=(userName, password))
except:
    print("Neo4j Server NOT started. Please start Neo4j and try again.")
    sys.exit(1)

print("Deleting graph")
graph.delete_all()
print("Finished deleting graph")

if USE_SQL:
    connection = sqlite3.connect(os.path.join(BASE_PATH, "qv_costbuilder.db"))
    cursor = connection.cursor()
    print("DROP and CREATE empty qv_prices and qv_blocks tables")
    query = "DROP TABLE IF EXISTS qv_prices"
    cursor.execute(query)
    query = "DROP TABLE IF EXISTS qv_blocks"
    cursor.execute(query)
    query = "DROP TABLE IF EXISTS qv_bldgs"
    cursor.execute(query)
    # qv_prices
    sql_command = """CREATE TABLE "qv_prices" ( "id" TEXT, "name" TEXT, "source" TEXT, "uom" TEXT, "price" REAL, "area" TEXT, "city" TEXT, "file" TEXT );"""
    cursor.execute(sql_command)
    prices_format_str = """INSERT INTO qv_prices (id, name, source, uom, price, area, city, file)
    VALUES ("{id}", "{name}", "{source}", "{uom}", {price}, "{area}", "{city}", "{file}");"""
    # qv_blocks
    sql_command = """CREATE TABLE "qv_blocks" ( "row_in_block" INTEGER, "last_item_id" TEXT, "id" TEXT, "uom" TEXT, "new_margin" REAL, "new_discount" REAL, "new_waste" REAL, "new_qty" REAL, "new_factor" REAL, "include_in_lbr_hrs" INTEGER, "include_in_plant_costs" INTEGER, "include_in_material_costs" INTEGER, "file" TEXT )"""
    cursor.execute(sql_command)
    blocks_format_str = """INSERT INTO qv_blocks (row_in_block, last_item_id, id, uom, new_margin, new_discount, new_waste, new_qty, new_factor, include_in_lbr_hrs, include_in_plant_costs, include_in_material_costs, file)
    VALUES ({row_in_block}, "{last_item_id}", "{id}", "{uom}", {new_margin}, {new_discount}, {new_waste}, {new_qty}, {new_factor}, {include_in_lbr_hrs}, {include_in_plant_costs}, {include_in_material_costs}, "{file}");"""
    # qv_bldgs
    sql_command = """CREATE TABLE "qv_bldgs" ( "bldg_id" TEXT, "id" TEXT, "uom" TEXT, "qty" REAL, "factor" REAL )"""
    cursor.execute(sql_command)
    bldgs_format_str = """INSERT INTO qv_bldgs (bldg_id, id, uom, qty, factor)
    VALUES ("{bldg_id}", "{id}", "{uom}", {qty}, {factor});"""


# Read in lookup tables
factor_lookup = pd.read_excel(os.path.join(INPUTS_PATH, 'factor_lookup.xlsx'))
print("factor_lookup: {} rows".format(len(factor_lookup)))
formula_lookup = pd.read_excel(os.path.join(INPUTS_PATH, 'formula_lookup.xlsx'))
print("formula_lookup: {} rows".format(len(formula_lookup)))
qty_lookup = pd.read_excel(os.path.join(INPUTS_PATH, 'qty_lookup.xlsx'))
print("qty_lookup: {} rows".format(len(qty_lookup)))


print("Start loading prices from {}".format(PATH_TO_PRICES_XLSX))

xlsx_files = os.listdir(PATH_TO_PRICES_XLSX)
xlsx_files = [item for item in xlsx_files if item.endswith('.xlsx')]
xlsx_files = [item for item in xlsx_files if not item.startswith('~')]
print(xlsx_files)

dfs = {}
for file in xlsx_files:
    print("Reading {}".format(file))
    stem = file.split('.')[0]
    dfs[stem] = pd.read_excel(os.path.join(PATH_TO_PRICES_XLSX, file))
    print(stem, len(dfs[stem]))
    print(dfs[stem][:2])

    # Capture list of Labour ids as that file passes by
    if file.startswith('LAB'):
        labour_ids = pd.DataFrame(dfs[stem][['id']].groupby('id').size())
        labour_ids.reset_index(inplace=True)
        labour_ids['include_in_lbr_hrs'] = 1
        labour_ids = labour_ids[['id','include_in_lbr_hrs']]
        print("Found {} labour ids to include in labour hours".format(len(labour_ids)))
    # Capture list of Plant ids as that file passes by
    if file.startswith('PLT'):
        plant_ids = pd.DataFrame(dfs[stem][['id']].groupby('id').size())
        plant_ids.reset_index(inplace=True)
        plant_ids['include_in_plant_costs'] = 1
        plant_ids = plant_ids[['id','include_in_plant_costs']]
        print("Found {} plant ids to include in plant costs".format(len(plant_ids)))

"""
['MAT_Master 2018Q3.xlsx', 'PRE1.xlsx', 'GLU.xlsx', 'SUP_ALL.xlsx', 'LAB1.xlsx', 'DRA.xlsx', 'PLT.xlsx', 'GEN_1.xlsx', 'PLU.xlsx']
Reading MAT_Master 2018Q3.xlsx
MAT_Master 2018Q3 965
        id                            name                          source uom  price
0  TBLC58T  Baluster C 860x58x58 H3.1 Pine  Bungalow and Villa online shop  ea  18.77
1  TBLC65T  Baluster C 860x65x65 H3.1 Pine  Bungalow and Villa online shop  ea  22.33
Reading PRE1.xlsx
PRE1 110
            id                              name          source uom  price
0  TI 200 unit  Timber Infill System_TI 200 unit  Precast Supply  m2  68.94
1  TI 225 unit  Timber Infill System_TI 225 unit  Precast Supply  m2  73.33
Reading GLU.xlsx
GLU 224
                  id                                        name             source uom  price
0  GL08_H1SS_042x180  42x180 Vis GL8 H1.2 PR Beams Sealed Sanded  GL08 Visual H1.2    m  34.75
1  GL08_H1SS_042x190  42x190 Vis GL8 H1.2 PR Beams Sealed Sanded  GL08 Visual H1.2    m  42.45
Reading SUP_ALL.xlsx
SUP_ALL 1713
             id                       name                source uom  AREA1  AREA2  AREA3  AREA4  AREA5  AREA6  AREA7
0  PM_01TIM_001   75 X  50 RAD NO1 H1.2 PG  01 Structural Timber  LM   4.17   4.17   3.00   3.28   3.95   3.29   3.29
1  PM_01TIM_004  150 X  50 RAD NO1 H1.2 PG  01 Structural Timber  LM  11.92  14.53  11.23  12.06  13.04  14.54  13.10
Reading LAB1.xlsx
LAB1 32
           id        name  source   uom  price
0  Blocklayer  Blocklayer  Labour  hour  65.14
1  Bricklayer  Bricklayer  Labour  hour  65.14
Reading DRA.xlsx
DRA 892
     id                       name                   source uom  price
0  2502  MHR1050.900 MANHOLE RISER  Manhole Riser 1050-1350  ea  542.8
1  2512  MHR1200.900 MANHOLE RISER  Manhole Riser 1050-1350  ea  678.5
Reading PLT.xlsx
PLT 134
              id                                           name         source uom  price
0  0508HireCivil    Civil Hire, based on 200m plus, 6-12 months  0508TempFence   m    2.0
1    0508HireCom  Commercial Hire, based on 50-200m, 6-9 months  0508TempFence   m    2.5
Reading GEN_1.xlsx
GEN_1 98
      id                                         name      source uom  price
0   BEDP                             Pipe Bedding 7mm  Aggregates  m3  29.91
1  GAP20  General All Passing 20 - lower end of range  Aggregates  m3  38.40
Reading PLU.xlsx
PLU 1224
       id                                               name                      source uom   price
0    3510  Arboles Wall Mounted Eye Wash                 ...      PlumbWorld TPW FAT COT  ea  733.04
1  316381  Pipe Cover Assembly Painted - 24 & 26 ltr     ...  PlumbWorld HWU RHM ACC      ea  233.04
"""

print("Start loading prices from {}".format(PATH_TO_PRICED_ELEMENTS_XLSX))

xlsx_files = os.listdir(PATH_TO_PRICED_ELEMENTS_XLSX)
xlsx_files = [item for item in xlsx_files if item.endswith('.xlsx')]
xlsx_files = [item for item in xlsx_files if not item.startswith('~')]
print(xlsx_files)

for file in xlsx_files:
    print("Reading {}".format(file))
    stem = file.split('.')[0]
    priced_elements = pd.read_excel(os.path.join(PATH_TO_PRICED_ELEMENTS_XLSX, file))
    print(stem, len(priced_elements))
    print(priced_elements[:2])
    break

# Add City nodes
cities = {"AREA1":"Auckland",
"AREA2":"Wellington",
"AREA3":"Christchurch",
"AREA4":"Dunedin",
"AREA5":"Waikato",
"AREA6":"Palmerston North",
"AREA7":"Tauranga",
}
for area in cities.keys():
    query = "CREATE ({}:City {{name: '{}'}});".format(cities[area].lower().replace(" ","_"), cities[area])

    graph.run(query)
    print("Created {}".format(cities[area]))
print("Finished creating cities\n")

########################################################################################

query = "MATCH (n:Assign) DELETE n;"
graph.run(query)
print("Deleted any existing Assign node")


query = "CREATE (:Assign {name: 'Assign'});"
graph.run(query)
print("Created Assign node\n")

########################################################################################

print("Start adding raw nodes")
# Confirm that no DUPLICATE ids are introduced
# by updating this list and checking against it
# NOTE - ids collected here are STRINGS
unique_ids = []
unique_ids_sources = []
errors = []    

# Add Raw Priced nodes
tx = graph.begin()
for file in dfs.keys():
    print("Loading {}".format(file))

    for index, row in dfs[file].iterrows():
        if row['id'] != row['id']:
            continue
        if str(row['id']) in unique_ids:
            error = "ERROR: id {} found in both {} and {}".format(row['id'], 
                unique_ids_sources[unique_ids.index(row['id'])],
                file)
            errors.append(error)
            print(error)
            continue
            # sys.exit("ERROR: id {} found in both {} and {}".format(row['id'], 
            #     unique_ids_sources[unique_ids.index(row['id'])],
            #     file))
        else:
            unique_ids.append(str(row['id']))
            unique_ids_sources.append(file)
        tx.evaluate('''
          CREATE (a:Raw {id:$label1, name:$label2, source:$label3, uom:$label4})
        ''', parameters = {'label1': str(row['id']), 
                           'label2': row['name'], 
                           'label3': row['source'], 
                           'label4': row['uom']})
tx.commit()

# Index them by id
query = """
CREATE INDEX ON :Raw(id);
"""

graph.run(query)
print("Index on Raw(id)")
print("Finished adding raw nodes\n")


print("Start adding prices")
# Add Raw Prices for each city
for area in cities.keys():
    city = cities[area]
    print("Processing {}".format(city))
    for file in dfs.keys():
        print(" - {}".format(file))
        price_column = 'price'
        if area in dfs[file].columns:
            price_column = area
        # confirm that city is COVERED BY the price file
        if not price_column in dfs[file].columns:
            print("WARNING: {} is not a column in {}".format(area, file))
            break
        tx = graph.begin()
        for index, row in dfs[file].iterrows():
            if row['id'] != row['id']:
                continue
            if row[price_column] != row[price_column]:
                continue
            tx.evaluate('''
              MATCH (a:Raw {{id:$label1}}), (b:City {{name:'{}'}})
              MERGE (a)-[r:PRICE_IN {{price:$label2, uom:$label3}}]->(b)
            '''.format(city), parameters = {'label1': str(row['id']), 
                               'label2': row[price_column], 
                               'label3': row['uom']})
            if USE_SQL:
                # '{id}', '{name}', '{source}', '{uom}', {price}, '{area}', '{city}', '{file}'
                sql_command = prices_format_str.format(id=row['id'], 
                    name=str(row['name']).replace('"', '""'),
                    source=row['source'],
                    uom=row['uom'],
                    price=row[price_column],
                    area=area, 
                    city=cities[area],
                    file=file)
                cursor.execute(sql_command)
        if USE_SQL:
            connection.commit()
        tx.commit()

print("Finished adding prices\n")

# Check totals by city
query = """
MATCH (n:Raw)-[r:PRICE_IN]->(c:City) RETURN c.name as city, count(*) AS count_of_prices ORDER BY c.name
"""

query_df = graph.run(query).to_data_frame()
print(query_df)
query_df.to_csv('price_count_by_city.csv', index=False)
print("Wrote price_count_by_city.csv")

"""
(qv) John-Graves-Qrious:qv johngraves$ cat price_count_by_city.csv
city,count_of_prices
Auckland,2672
Christchurch,2672
Dunedin,2672
Palmerston North,2672
Tauranga,2672
Waikato,2672
Wellington,2672
"""


xlsx_files = os.listdir(PATH_TO_BLOCKS_XLSX)
xlsx_files = [item for item in xlsx_files if item.endswith('.xlsx')]
xlsx_files = sorted([item for item in xlsx_files if not item.startswith('~')])
print("blocks:", xlsx_files)

def add_new_columns(df, stem):
    df['row_in_file'] = df.index
    row_in_block = []
    block_row = 0
    for index, row in df.iterrows():
        if row['item_block'] == 'I':
            i_row = index
            block_row = 0
        else:
            block_row += 1
        row_in_block.append(block_row)
    df['row_in_block'] = row_in_block

    df2 = df.merge(formula_lookup[['qty','formula','discount','waste','margin','start_row','end_row']], 
                   on=['qty','formula'],
                   how='left')
    # catch any qty + formula pairs that DO NOT have a lookup value
    def check_row_for_equals_XGETVALUE_and_SUBTOTAL(row):
        if (type(row['qty']) != str) or (type(row['formula']) != str):
            return False
        if row['qty'].startswith('=XGETVALUE') and row['formula'].startswith('=SUBTOTAL'):
            return True
        else:
            return False
    df2['should_have_lookup'] = df2.apply(lambda row: check_row_for_equals_XGETVALUE_and_SUBTOTAL(row), axis=1)
    def check_row_for_lookup(row):
        if row['end_row'] == row['end_row']:
            return True
        else:
            return False
    df2['has_lookup'] = df2.apply(lambda row: check_row_for_lookup(row), axis=1)
    def check_row_for_correction_needed(row):
        if row['should_have_lookup'] == True  and row['has_lookup'] == False:
            return True
        else:
            return False
    df2['needs_correction'] = df2.apply(lambda row: check_row_for_correction_needed(row), axis=1)
    if len(df2[df2['needs_correction'] == True]) > 0:
        correction_file = "{}_needs_correction.csv".format(stem)
        df2[df2['needs_correction'] == True].to_csv(correction_file, index=False)
        error = " * * * * CORRECTION NEEDED in formula_lookup.xlsx * * * * see {}".format(correction_file)
        print(error)
        errors.append(error)

    df3 = df2.sort_values(by='row_in_file', ascending=False).copy()

    new_columns = {'margin': [],
                   'discount': [],
                   'waste': []}

    for index, row in df3.iterrows():
        if row['row_in_block'] == 0 or index == max(df3.index):
            carry_dict = {'margin':   {'value': 1, 'start_row': 0, 'end_row': 100},
                          'discount': {'value': 1, 'start_row': 0, 'end_row': 100},
                          'waste':    {'value': 1, 'start_row': 0, 'end_row': 100}}
            for item in ['margin', 'discount', 'waste']:
                new_columns[item].append(1)
            continue
        for item in ['margin', 'discount', 'waste']:
            if row[item] == row[item]:
                carry_dict[item]['value'] = row[item]
                carry_dict[item]['start_row'] = row['start_row']
                carry_dict[item]['end_row'] = row['end_row']
        if row['item_block'] == 'B':
            for item in ['margin', 'discount', 'waste']:
                if row[item] != row[item]:
                    if row['row_in_block'] >= carry_dict[item]['start_row'] and \
                       row['row_in_block'] <= carry_dict[item]['end_row']: 
                        new_columns[item].append(carry_dict[item]['value'])
                    else:
                        new_columns[item].append(1)
                else:
                    new_columns[item].append(1)
        else:
            for item in ['margin', 'discount', 'waste']:
                new_columns[item].append(1)

    for item in ['margin', 'discount', 'waste']:
        df3['new_'+item] = new_columns[item]

    df4 = df3.merge(qty_lookup[['qty','new_qty']], on='qty', how='left')

    # factor lookups only needed if factor of source file contained strings (object)
    df4_columns = list(df4.columns)
    df4_dtypes = list(df4.dtypes)
    factor_dtype = df4_dtypes[df4_columns.index('factor')]
    if factor_dtype == 'float64':
        df4['new_factor'] = df4['factor']
        df5 = df4.copy()
    else:
        df5 = df4.merge(factor_lookup[['factor','new_factor']], on='factor', how='left')

    df6 = df5.merge(labour_ids, on='id', how='left')
    df6['include_in_lbr_hrs'].fillna(0, inplace=True)
    df7 = df6.merge(plant_ids, on='id', how='left')
    df7['include_in_plant_costs'].fillna(0, inplace=True)
    def in_material_cost_if_not_plant_or_labour(row):
        if row['include_in_lbr_hrs'] == 1 or row['include_in_plant_costs'] == 1:
            return 0
        else:
            return 1
    df7['include_in_material_costs'] = df7.apply(lambda row: in_material_cost_if_not_plant_or_labour(row), axis=1)

    return df7.sort_values(by='row_in_file').copy()

dfs = {}
dfs_stats = {}
for file in xlsx_files:
    print("Reading {}".format(file))
    stem = file.split('.')[0]
    dfs[stem] = pd.read_excel(os.path.join(PATH_TO_BLOCKS_XLSX, file))
    print(stem, len(dfs[stem]))
    print(list(dfs[stem].columns))
    print(dfs[stem][:2])
    dfs[stem] = add_new_columns(dfs[stem][['item_block', 'id', 'name', 'qty', 'uom', 'formula', 'factor', 'costx1']], stem)
    print("After realignment:")
    print(list(dfs[stem].columns))
    print(dfs[stem][:2])
    # Obtain REQUIRES check figures of "B"+id
    dfs_stats[stem] = {}
    dfs_stats[stem]['B + id'] = len(dfs[stem][(dfs[stem]['item_block'] == 'B') & (dfs[stem]['id'] == dfs[stem]['id'])])
    print("Counting {} B + id in {}".format(dfs_stats[stem]['B + id'], stem))
    dfs_stats[stem]['B + id + qty'] = len(dfs[stem][(dfs[stem]['item_block'] == 'B') & (dfs[stem]['id'] == dfs[stem]['id']) \
       & (dfs[stem]['qty'] == dfs[stem]['qty'])])
    print("Counting {} B + id + qty in {}".format(dfs_stats[stem]['B + id + qty'], stem))
    dfs_stats[stem]['B + id + qty + costx1'] = len(dfs[stem][(dfs[stem]['item_block'] == 'B') & (dfs[stem]['id'] == dfs[stem]['id'])\
       & (dfs[stem]['qty'] == dfs[stem]['qty']) & (dfs[stem]['costx1'] == dfs[stem]['costx1'])])
    print("Counting {} B + id + qty + costx1 in {}".format(dfs_stats[stem]['B + id + qty + costx1'], stem))

"""
"""    

print("Start adding items")
# Add items
for file in dfs.keys():
  print("Processing node additions for {}".format(file))
  tx = graph.begin()
  for index, row in dfs[file].iterrows():
      if row['item_block'] == 'I':
          if str(row['id']) in unique_ids:
              error = "ERROR: id {} found in both {} and {}".format(row['id'], 
                  unique_ids_sources[unique_ids.index(row['id'])],
                file)
              errors.append(error)
              print(error)
          else:
              unique_ids.append(str(row['id']))
              unique_ids_sources.append(file)

          tx.evaluate('''CREATE (a:Item {id:$label1, name:$label2, uom:$label3})''', 
               parameters = {'label1': str(row['id']), 
                             'label2': row['name'], 
                             'label3': row['uom']})
  tx.commit()

# Index them by id
query = """
CREATE INDEX ON :Item(id);
"""

graph.run(query)
print("Index on item(id)")
print("Finished adding items\n")

"""
"""    

print("Start adding relationships")
# Add relationships
for dfs_index, file in enumerate(list(dfs.keys())):
    print("{}/{} Processing relationship additions for {}".format(dfs_index, len(dfs.keys()), file))
    f = open(os.path.join(PATH_TO_AUDIT, "_{}_.csv".format(file.strip())), 'w')
    tx = graph.begin()
    last_item_id = ""
    for index, row in dfs[file].iterrows():
        if row['item_block'] == 'O':
            # Omit
            continue
        if row['item_block'] == 'I':
            f.close()
            last_item_id = row['id']
            print(last_item_id)
            f = open(os.path.join(PATH_TO_AUDIT, "{}.{}.csv".format(last_item_id.strip(), file)), 'w')
            continue
        elif row['item_block'] == 'B':
            if row['id'] != row['id']:
                continue
            if row['qty'] != row['qty']:
                continue
            if row['costx1'] != row['costx1']:
                continue

            # Use merged new_qty if available
            if row['new_qty'] == row['new_qty']:
                qty = row['new_qty']
            else:
                qty = row['qty']

            if type(qty) == str:
                try:
                    qty = float(qty)
                except ValueError as e:
                    print(e, row['qty'])
                    error = "ERROR: ValueError on id {} for ,qty, of ,{}, and ,new_qty, of ,{}, in ,{}".format(row['id'], 
                        row['qty'], 
                        row['new_qty'], 
                        file)
                    print(error)
                    errors.append(error)

            # Use merged new_factor if available
            if row['new_factor'] == row['new_factor']:
                factor = row['new_factor']
            else:
                factor = row['factor']

            if type(factor) == str:
                try:
                    factor = float(factor)
                except ValueError as e:
                    print(e)
                    error = "ERROR: ValueError on id {} for ,factor, of ,{}, and ,new_factor, of ,{}, in ,{}".format(row['id'], 
                        row['factor'],  
                        row['new_factor'], 
                        file)
                    print(error)
                    errors.append(error)

            if factor != factor:
                error = "ERROR: factor is NaN on id {} in {}".format(row['id'], file)
                print(error)
                errors.append(error)

            print(" - {}".format(row['id']))
            f.write("{},{},{},{},{},{},{},{},{},{},{}\n".format(last_item_id,
                row['id'],
                qty,
                row['uom'],
                factor,
                row['new_discount'],
                row['new_waste'],
                row['new_margin'],
                row['include_in_lbr_hrs'],
                row['include_in_plant_costs'],
                row['include_in_material_costs']))
            # Node to match MUST ALREADY EXISTS in graph
            if not (str(row['id']) in unique_ids):
                error = "ERROR: id {} in file {} is not on graph".format(row['id'], file)
                print(error)
                errors.append(error)
            # Match any raw nodes required
            tx.evaluate('''
              MATCH (a:Item {id:$label1}), (b:Raw {id:$label2})
              CREATE (a)-[r:REQUIRES {qty:$label3, uom:$label4, factor:$label5, discount:$label6, waste:$label7, margin:$label8, include_in_lbr_hrs:$label9, include_in_plant_costs:$label10, include_in_material_costs:$label11}]->(b)
            ''', parameters = {'label1': str(last_item_id),
                               'label2': str(row['id']),
                               'label3': qty,
                               'label4': row['uom'],
                               'label5': factor,
                               'label6': row['new_discount'],
                               'label7': row['new_waste'],
                               'label8': row['new_margin'],
                               'label9': row['include_in_lbr_hrs'],
                               'label10': row['include_in_plant_costs'],
                               'label11': row['include_in_material_costs']})
            # Match any item nodes required
            tx.evaluate('''
              MATCH (a:Item {id:$label1}), (b:Item {id:$label2})
              CREATE (a)-[r:REQUIRES {qty:$label3, uom:$label4, factor:$label5, discount:$label6, waste:$label7, margin:$label8, include_in_lbr_hrs:$label9, include_in_plant_costs:$label10, include_in_material_costs:$label11}]->(b)
            ''', parameters = {'label1': str(last_item_id),
                               'label2': str(row['id']),
                               'label3': qty,
                               'label4': row['uom'],
                               'label5': factor,
                               'label6': row['new_discount'],
                               'label7': row['new_waste'],
                               'label8': row['new_margin'],
                               'label9': row['include_in_lbr_hrs'],
                               'label10': row['include_in_plant_costs'],
                               'label11': row['include_in_material_costs']})
            if USE_SQL:
                # {row_in_block}, "{last_item_id}", "{id}", "{uom}", {new_margin}, {new_discount}, {new_waste}, {new_qty}, {new_factor}, {include_in_lbr_hrs}
                sql_command = blocks_format_str.format(row_in_block=row['row_in_block'],
                    last_item_id=last_item_id,
                    id=row['id'],
                    uom=row['uom'],
                    new_margin=row['new_margin'],
                    new_discount=row['new_discount'],
                    new_waste=row['new_waste'],
                    new_qty=qty,
                    new_factor=factor,
                    include_in_lbr_hrs=row['include_in_lbr_hrs'],
                    include_in_plant_costs=row['include_in_plant_costs'],
                    include_in_material_costs=row['include_in_material_costs'],
                    file=file)
                cursor.execute(sql_command)

    if USE_SQL:
        connection.commit()
    tx.commit()

    # Get count of REQUIRES (cumulative count)
    query = "MATCH (n)-[r:REQUIRES]->() RETURN COUNT(r) as count"

    query_df = graph.run(query).to_data_frame()
    query_df['file'] = file
    if dfs_index == 0:
        cum_REQUIRES_df = query_df.copy()
    else:
        cum_REQUIRES_df = pd.concat([cum_REQUIRES_df, query_df])

f.close()
print("Finished adding relationships\n")


########################################################################################

# Try to patch up pricing relationships missed

df = pd.read_excel(os.path.join(PATH_TO_ASSIGNED_XLSX, 'assigned.xlsx'))
print("Count of prices to assign:", len(df))
print(df[:2])

"""
1911
           id       price  uom
0  T26.09.017  125.548348   m2
1  T26.11.009   74.187660   m2
"""

tx = graph.begin()
for index, row in df.iterrows():
    tx.evaluate('''
      MATCH (a:Item {id:$label1}), (b:Assign)
      MERGE (a)-[r:PRICE_IN {price:$label2, uom:$label3}]->(b)
    ''', parameters = {'label1': str(row['id']), 
                       'label2': row['price'], 
                       'label3': row['uom']})
tx.commit()

# Check count of assigned prices
query = """
MATCH (n:Item)-[r:PRICE_IN]->(a:Assign) RETURN count(n) AS count_of_assigned
"""

query_df = graph.run(query).to_data_frame()
print("Count of prices assigned:", query_df['count_of_assigned'][0])

########################################################################################

# Try to fill in embedded element prices

print("Count of element prices to assign:", len(priced_elements))
print(priced_elements[:2])

"""
NOTE price column is called "product":
priced_elements 206
  item_block          id                               name  qty  uom   rate  factor  product
0          I  E19.22.142                       Multi-storey    1  NaN  25.31     1.0    25.31
1          I  E19.22.172  Individual house, medium standard    1  NaN  30.43     1.0    30.43
"""

tx = graph.begin()
for index, row in priced_elements.iterrows():
    tx.evaluate('''
      MATCH (a:Item {id:$label1}), (b:Assign)
      MERGE (a)-[r:PRICE_IN {price:$label2, uom:$label3}]->(b)
      SET a.source = "priced_elements"
    ''', parameters = {'label1': str(row['id']), 
                       'label2': row['product'], 
                       'label3': row['uom']})
tx.commit()

# Check count of assigned prices
query = """
MATCH (n:Item) WHERE EXISTS(n.total_price) AND n.source = "priced_elements" RETURN count(n) AS count_of_assigned
"""

query_df = graph.run(query).to_data_frame()
print("Count of element prices assigned:", query_df['count_of_assigned'][0])

########################################################################################

print("\nStart processing workbooks from {}\n".format(PATH_TO_WORKBOOKS_XLSX))

xlsx_files = os.listdir(PATH_TO_WORKBOOKS_XLSX)
xlsx_files = [item for item in xlsx_files if item.endswith('.xlsx')]
xlsx_files = [item for item in xlsx_files if not item.startswith('~')]
print(xlsx_files)

required_count_by_building = {}
for file in xlsx_files:
    node_name = ("Bldg " + file.split(',')[0]).replace(" ","_") # 1 House B1 -> Bldg_1_House_B1
    print(" - {}".format(node_name), end=" ")
    df = pd.read_excel(os.path.join(PATH_TO_WORKBOOKS_XLSX, file), header=None)
    df2 = df.iloc[4:,0:7].copy()
    df2.columns = ['id','description','qty','uom','rate','subtotal','factor']
    df2.dropna(subset=['id','qty'], inplace=True)
    del(df2['rate'])
    del(df2['subtotal'])
    df3 = df2[df2['uom'] != '%'].copy()
    df4 = df3[df3['id'].apply(lambda x: len(x)>3)].copy()
    print("{} items required".format(len(df4)))
    required_count_by_building[node_name] = len(df4)
    tx = graph.begin()
    # Add building node
    tx.evaluate('''
    CREATE (a:Bldg {id:$label1})
    ''', parameters = {'label1': str(node_name)})
    for index, row in df4.iterrows():
        # Match any item nodes required
        tx.evaluate('''
          MATCH (a:Bldg {id:$label1}), (b:Item {id:$label2})
          MERGE (a)-[r:REQUIRES {qty:$label3, uom:$label4, factor:$label5, margin:1, waste:1, discount:1}]->(b)
        ''', parameters = {'label1': str(node_name),
                           'label2': str(row['id']),
                           'label3': row['qty'],
                           'label4': row['uom'],
                           'label5': row['factor']})

        if USE_SQL:
            # {bldg_id}, {id}, {uom}, {qty}, {factor}
            sql_command = bldgs_format_str.format(bldg_id=node_name,
                id=row['id'],
                uom=row['uom'],
                qty=row['qty'],
                factor=row['factor'])
            cursor.execute(sql_command)
    tx.commit()

    if USE_SQL:
        connection.commit()

    # Get count of building REQUIRES (cumulative count)
    query = "MATCH (b:Bldg)-[:REQUIRES]->() RETURN DISTINCT b.id as bldg_id, SIZE( (b)-[:REQUIRES]->() ) as required_count"

    query_df = graph.run(query).to_data_frame()
    query_df['input_count'] = query_df['bldg_id'].apply(lambda x: required_count_by_building[x])
    query_df['missing'] = query_df['input_count'] - query_df['required_count']
    query_df.to_csv('bldg_REQUIRES_df.csv', index=False)


print("Finished processing workbooks\n")

if USE_SQL:
    connection.close()

cum_REQUIRES_df['prior_count'] = cum_REQUIRES_df['count'].shift(1)
cum_REQUIRES_df['contribution'] = cum_REQUIRES_df['count'] - cum_REQUIRES_df['prior_count']
count1 = []
count2 = []
count3 = []
for index, row in cum_REQUIRES_df.iterrows():
    count1.append(dfs_stats[row['file']]['B + id'])
    count2.append(dfs_stats[row['file']]['B + id + qty'])
    count3.append(dfs_stats[row['file']]['B + id + qty + costx1'])
cum_REQUIRES_df['B_id'] = count1
cum_REQUIRES_df['B_id_qty'] = count2
cum_REQUIRES_df['B_id_qty_costx1'] = count3
missing = []
for index, row in cum_REQUIRES_df.iterrows():
    if index == 0:
        missing.append(row['B_id_qty_costx1'] - row['count'])
    else:
        missing.append(row['B_id_qty_costx1'] - row['contribution'])
cum_REQUIRES_df['missing'] = missing
cum_REQUIRES_df.to_csv('cum_REQUIRES_df.csv', index=False)
print("Output cum_REQUIRES_df.csv to audit relationships added\n")


if AGGREGATE_UP_AUCKLAND == True:
    aggregate_up_the_levels(city="Auckland", area="AREA1", graph=graph)


if len(errors) > 0:
    print("*************** {} ERRORS ***************".format(len(errors)))
    for error in errors:
        print(error)
    print("*************** {} ERRORS ***************".format(len(errors)))