# aggregate up the levels
# 20190117 1413 extracted as separate function

def aggregate_up_the_levels(city="Auckland", area="AREA1", graph=None):
    if graph == None:
        print("No graph")
        return
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
    # Clear any sub raw cost or total cost properties
    for type_of_price in ['raw_material', 'total_material', 
                          'raw_plant', 'total_plant', 
                          'raw_labour', 'total_labour']:
        query = """MATCH (n) WHERE EXISTS(n.{type_of_price}_cost) REMOVE n.{type_of_price}_cost""".format(type_of_price=type_of_price)
        query_result = graph.run(query)

    # Check count of costs of current graph
    print("\nCheck")
    for type_of_price in ['raw_material', 'total_material', 
                          'raw_plant', 'total_plant', 
                          'raw_labour', 'total_labour']:
        query = """MATCH (n) WHERE EXISTS(n.{type_of_price}_cost) RETURN count(n) as count""".format(type_of_price=type_of_price)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        print("Count of nodes with {}_cost: {}".format(type_of_price, count_of_nodes))

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

    grand_total_nodes = 0
    for type_of_node in ['Item', 'Bldg']:
        print("\nCheck")
        total_nodes = 0
        for level in range(1, max_level+1):
            query = """MATCH (n:{}) WHERE n.level={} RETURN count(n) as count""".format(type_of_node, level)

            query_df = graph.run(query).to_data_frame()
            count_of_nodes = query_df['count'][0]
            total_nodes += count_of_nodes
            grand_total_nodes += count_of_nodes
            print("Count of {} nodes with level of {}: {}".format(type_of_node, level, count_of_nodes))
        print("Total {} nodes with levels: {}".format(type_of_node, total_nodes))    
    print("Total nodes with levels: {}".format(grand_total_nodes))    

    ####################################################################################

    print("\n" + "="*80)
    print("Set prices\n")

    # STEP 0 Set assigned_prices AS raw_prices (can be overwritten in next step)
    # TODO - what is material/plant/labour breakdown of assigned prices? Put all into material for now JG 20190117 1522 
    type_of_price = 'assigned'
    query = """MATCH (i:Item)-[price_in:PRICE_IN]->(j:Assign)
    WITH i, price_in.price AS assigned_price
    SET i.assigned_price = assigned_price
    SET i.raw_price = assigned_price
    SET i.raw_material_cost = assigned_price
    SET i.raw_plant_cost = 0
    SET i.raw_labour_cost = 0
    RETURN count(i) as count"""

    query_df = graph.run(query).to_data_frame()
    count_of_nodes = query_df['count'][0]
    print("Count of nodes with {}_price (also raw): {}".format(type_of_price, count_of_nodes))

    # STEP 1 Set raw_prices
    type_of_price = 'raw'
    query = """MATCH (i:Item)-[r:REQUIRES]->(j:Raw)-[price_in:PRICE_IN]->(:City {{name: '{}'}})
    WITH i, sum(r.qty * r.discount * r.factor * r.margin * r.waste * price_in.price) AS raw_price,
            sum(r.qty * r.discount * r.factor * r.margin * r.waste * price_in.price * r.include_in_material_costs) AS raw_material_cost,
            sum(r.qty * r.discount * r.factor * r.margin * r.waste * price_in.price * r.include_in_plant_costs) AS raw_plant_cost,
            sum(r.qty * r.discount * r.factor * r.margin * r.waste * price_in.price * r.include_in_lbr_hrs) AS raw_labour_cost, 
            sum(r.qty * r.factor * coalesce(r.include_in_lbr_hrs,0)) as lbr_hrs
    SET i.raw_price = raw_price
    SET i.raw_material_cost = raw_material_cost
    SET i.raw_plant_cost = raw_plant_cost
    SET i.raw_labour_cost = raw_labour_cost
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
    coalesce(i.lbr_hrs,0) + sum(s.qty * s.factor * coalesce(j.lbr_hrs,0)) as lbr_hrs
    SET i.total_price = total_price
    SET i.lbr_hrs = lbr_hrs
    RETURN count(i) as count"""

    query_df = graph.run(query).to_data_frame()
    count_of_nodes = query_df['count'][0]
    print("Count of nodes with {}_price: {}".format(type_of_price, count_of_nodes))

    # STEP 2b Some nodes will have BOTH a raw cost and, because they use other items as well, a total cost
    for item in [('total_material_cost', 'raw_material_cost'),
                 ('total_plant_cost', 'raw_plant_cost'),
                 ('total_labour_cost', 'raw_labour_cost')]:
        query = """MATCH (i:Item)-[s:REQUIRES]->(j:Item)
        WHERE EXISTS(i.{raw}) AND EXISTS(j.{raw})
        WITH i, i.{raw} + sum(s.qty * s.discount * s.factor * s.margin * s.waste * j.{raw}) as {total}
        SET i.{total} = {total}
        RETURN count(i) as count""".format(total=item[0], raw=item[1])

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        print("Count of nodes with {total}: {count}".format(total=item[0], count=count_of_nodes))

    # STEP 3 Now we need to fill in the blanks for total price
    type_of_price = 'total'
    query = """MATCH (i:Item)
    WHERE NOT EXISTS(i.total_price) AND EXISTS(i.raw_price)
    SET i.total_price = i.raw_price
    RETURN count(i) as count"""

    query_df = graph.run(query).to_data_frame()
    count_of_nodes = query_df['count'][0]
    print("Count of nodes with {}_price added AS raw_price: {}".format(type_of_price, count_of_nodes))

    # STEP 3b Now we need to fill in the blanks for sub costs
    for item in [('total_material_cost', 'raw_material_cost'),
                 ('total_plant_cost', 'raw_plant_cost'),
                 ('total_labour_cost', 'raw_labour_cost')]:
        query = """MATCH (i:Item)
        WHERE NOT EXISTS(i.{total}) AND EXISTS(i.{raw})
        SET i.{total} = i.{raw}
        RETURN count(i) as count""".format(total=item[0], raw=item[1])

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        print("Count of nodes with {total} added AS {raw}: {count}".format(total=item[0], raw=item[1], count=count_of_nodes))

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

    for item in ['total_price', 'total_material_cost', 'total_plant_cost', 'total_labour_cost']:
        print("\nCheck")
        total_nodes = 0
        for level in range(1, max_level+1):
            query = """MATCH (n:Item) WHERE n.level={} AND EXISTS(n.{}) RETURN count(n) as count""".format(level, item)

            query_df = graph.run(query).to_data_frame()
            count_of_nodes = query_df['count'][0]
            total_nodes += count_of_nodes
            print("Count of nodes with {} and level of {}: {}".format(item, level, count_of_nodes))
        print("Total nodes with {} and levels: {}".format(item, total_nodes))

    # STEP 4 Now we can fill in the next levels of required total prices
    # NOTE ALL j must have total price
    for level in range(1, max_level+1):
        print("\nAdd level {}".format(level))
        type_of_price = 'total'
        query = """MATCH (i)-[s:REQUIRES]->(j)
        WHERE i.level = {} AND NOT EXISTS(i.total_price)
        WITH i, 
        sum(s.qty * s.discount * s.factor * s.margin * s.waste * j.total_price) as total_price, 
        sum(s.qty * s.discount * s.factor * s.margin * s.waste * coalesce(j.total_material_cost,0)) as total_material_cost, 
        sum(s.qty * s.discount * s.factor * s.margin * s.waste * coalesce(j.total_plant_cost,0)) as total_plant_cost, 
        sum(s.qty * s.discount * s.factor * s.margin * s.waste * coalesce(j.total_labour_cost,0)) as total_labour_cost, 
        sum(s.qty * s.factor * coalesce(j.lbr_hrs,0)) as lbr_hrs
        SET i.total_price = total_price
        SET i.total_material_cost = total_material_cost
        SET i.total_plant_cost = total_plant_cost
        SET i.total_labour_cost = total_labour_cost
        SET i.lbr_hrs = lbr_hrs
        RETURN count(i) as count""".format(level)

        query_df = graph.run(query).to_data_frame()
        count_of_nodes = query_df['count'][0]
        print("Count of nodes with {}_price added at level {}: {}".format(type_of_price, level, count_of_nodes))

    for item in ['total_price', 'total_material_cost', 'total_plant_cost', 'total_labour_cost']:
        print("\nCheck")
        total_nodes = 0
        for level in range(1, max_level+1):
            query = """MATCH (n) WHERE n.level={} AND EXISTS(n.{}) RETURN count(n) as count""".format(level, item)

            query_df = graph.run(query).to_data_frame()
            count_of_nodes = query_df['count'][0]
            total_nodes += count_of_nodes
            print("Count of nodes with {} and level of {}: {}".format(item, level, count_of_nodes))
        print("Total nodes with {} and levels: {}".format(item, total_nodes))

        print("\nCheck")
        total_nodes = 0
        for level in range(1, max_level+1):
            query = """MATCH (n:Item) WHERE n.level={} AND EXISTS(n.{}) RETURN count(n) as count""".format(level, item)

            query_df = graph.run(query).to_data_frame()
            count_of_nodes = query_df['count'][0]
            total_nodes += count_of_nodes
            print("Count of Item nodes with {} and level of {}: {}".format(item, level, count_of_nodes))
        print("Total Item nodes with {} and levels: {}".format(item, total_nodes))


    print("Finished aggregating up the levels")
