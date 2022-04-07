from functools import reduce


with open('./datasets/order_items/part-00000') as orderItemsFies:
    # print(type(orderItemsFies))
    readDataAsString = orderItemsFies.read()
    # print(type(readDataAsString))
    # print(readDataAsString[:93])

    #convert it into a list of records
    orderItemsList = readDataAsString.splitlines() #splits on newline('\n')
    # print(len(orderItemsList))
    # print(orderItemsList[:10]) #first 10 records
    # print(orderItemsList[-10: -1]) #last 10 records

    #get all order with order id 68880
    filtered_orders = list(filter(lambda x: int(x.split(',')[1])==68880, orderItemsList))
    # print(filtered_orders)

    map_order_amount = list(map(lambda x: float(x.split(',')[4]), filtered_orders))
    # print(map_order_amount)

    total_order_amount = reduce(lambda x, y: x+y, map_order_amount)
    print(total_order_amount)

