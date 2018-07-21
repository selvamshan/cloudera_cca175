

pyspark --master yarn --conf spark.ui.port=12888


order_items = sc.textFile('/public/retail_db/order_items')
for i in order_items.take(10): print (i)
order_items.first()
order_items.count()


order_items_map = order_items. \
map(lambda oi: (int(oi.split(',')[1]), float(oi.split(',')[4])))

revenue_per_order = order_items_map. \
reduceByKey(lambda x, y: x + y)

for i in revenue_per_order.take(10): print(i)


product_raw = open("/data/retail_db/products/part-00000").read().splitlines()

products_rdd = sc.parallelize(product_raw)



sqlContext.load("/public/retail_db_json/order_items", "json").show()

sqlContext.read.json("/public/retail_db_json/order_items").show()

orders = sc.textFile("/public/retail_db/orders")
for i in orders.take(10): print(i)

order_status_map = orders.map(lambda o: (o.split(',')[3], 1))
for i in order_status_map.take(10): print(i)

order_items = sc.textFile('/public/retail_db/order_items')
for i in order_items.take(10): print (i)

order_items_map = order_items. \
map(lambda oi: (int(oi.split(',')[1]), float(oi.split(',')[4])))
for i in order_items_map.take(10): print(i)


#12 vedio
orders = sc.textFile("/public/retail_db/orders")
for i in orders.take(10): print(i)

orders_complete = orders.filter(lambda o: o.split(',')[3] == 'COMPLETE')
for i in orders_complete.take(10): print(i)

print(orders.count(), orders_complete.count())

orders_complete = orders. \
filter(lambda o: 
   (o.split(',')[3] == 'COMPLETE' or o.split(',')[3] == 'CLOSED') and
    o.split(',')[1][:7] == '2014-01')
for i in orders_complete.take(10): print(i)

orders_complete = orders. \
filter(lambda o: 
   o.split(',')[3] in ['COMPLETE','CLOSED'] and  o.split(',')[1][:7] == '2014-01')
for i in orders_complete.take(10): print(i)

#14 joins
orders = sc.textFile("/public/retail_db/orders")
order_items = sc.textFile('/public/retail_db/order_items')


orders_map = orders. \
map(lambda o: (int(o.split(',')[0]), o.split(',')[1]))
for i in orders_map.take(5):print(i)


order_items_map = order_items. \
map(lambda oi: (int(oi.split(',')[1]), float(oi.split(',')[4])))
for i in order_items_map.take(10): print(i)

orders_join = orders_map.join(order_items_map)
for i in orders_join.take(10):print(i)

orders_left_join = orders_map.leftOuterJoin(order_items_map)
for i in orders_left_join.take(10):print(i)

orders_map = orders. \
map(lambda o: (int(o.split(',')[0]), o.split(',')[3]))

orders_left_join = orders_map.leftOuterJoin(order_items_map)
for i in orders_left_join.take(10):print(i)

orders_left_join_filter = orders_left_join. \
filter(lambda o: o[1][1] == None)
for i in orders_left_join_filter.take(10):print(i)

orders_left_join_filter.count()

#17 aggregation
order_items = sc.textFile('/public/retail_db/order_items')
order_items.count()

#Aggregation  -total -get revenue for given order_id
for i in order_items.take(10): print (i)

order_items_filter = order_items. \
filter(lambda oi: int(oi.split(',')[1]) ==  2)
for i in order_items_filter.take(10): print(i)

order_items_subtotal_map = order_items_filter. \
map(lambda oi: float(oi.split(',')[4]))
for i in order_items_subtotal_map.take(10): print(i)

order_items_subtotals = order_items_subtotal_map. \
reduce(lambda x, y: x + y)
print(order_items_subtotals)

from operator import add
order_items_subtotals =  order_items. \
filter(lambda oi: int(oi.split(',')[1]) ==  2). \
map(lambda oi: float(oi.split(',')[4])). \
reduce(add)
print(order_items_subtotals)

#18
# getting minimum subtotal for perticular order id
order_items = sc.textFile('/public/retail_db/order_items')
order_item_min_subtoal = order_items. \
filter(lambda oi: int(oi.split(',')[1]) ==  2). \
map(lambda oi: float(oi.split(',')[4])). \
reduce(lambda x, y: min(x, y))
print(order_item_min_subtoal)

order_item_min_subtoal = order_items. \
filter(lambda oi: int(oi.split(',')[1]) ==  2). \
map(lambda oi: float(oi.split(',')[4])). \
reduce(lambda x, y: x if x < y else y)
print(order_item_min_subtoal)


#count by status
orders = sc.textFile('/public/retail_db/orders')
for i in orders.take(10):print(i)

order_status_cnt = orders. \
map(lambda o: (o.split(',')[3], 1)). \
countByKey()
for i, j in order_status_cnt.items(): print(i,j)

# 20toal revenue per oder id
revenue_per_oder_id =  order_items. \
map(lambda oi: (int(oi.split(',')[1]), float(oi.split(',')[4]))). \
groupByKey()
for i in revenue_per_oder_id.take(20): print(i[0], list(i[1]))

revenue_per_oder_id =  order_items. \
map(lambda oi: (int(oi.split(',')[1]), float(oi.split(',')[4]))). \
groupByKey(). \
map(lambda oi: (oi[0], round(sum(oi[1]),2)))
for i in revenue_per_oder_id.take(20): print(i)

#21 get order items detials in decsending order by revenue -groupByKey
order_items = sc.textFile('/public/retail_db/order_items')
for i in order_items.take(10): print(i)

order_items_map = order_items. \
map(lambda oi: (int(oi.split(',')[1]), oi))
for i in order_items_map.take(10): print(i)

order_items_groupby_orderid = order_items_map.groupByKey()
for i in order_items_groupby_orderid.take(10): print(i)

order_item_sorted_subtotal_per_order = order_items_groupby_orderid. \
flatMap(lambda oi: 
    sorted(oi[1], key= lambda k: float(k.split(',')[4]), reverse = True)
    )
for i in order_item_sorted_subtotal_per_order.take(10): print(i)



order_item_sorted_subtotal_per_order = order_items. \
map(lambda oi: (int(oi.split(',')[1]), oi)). \
groupByKey(). \
flatMap(lambda oi: 
    sorted(oi[1], key= lambda k: float(k.split(',')[4]), reverse = True)
    )
for i in order_item_sorted_subtotal_per_order.take(10): print(i)


revenue_per_oder_id =  order_items. \
map(lambda oi: (int(oi.split(',')[1]), float(oi.split(',')[4]))). \
reduceByKey(lambda x, y: x + y)
for i in revenue_per_oder_id.take(20): print(i)

#get revenue and count of items for each order id
order_items = sc.textFile('/public/retail_db/order_items')
for i in order_items.take(10): print(i)

order_item_map = order_items. \
map(lambda oi: (int(oi.split(',')[1]),float(oi.split(',')[4])))
for i in order_item_map.take(10): print(i)

revenue_per_order = order_item_map. \
aggregateByKey((0.0, 0), 
    lambda x, y: (x[0] + y, x[1] +1),
    lambda x, y: (x[0] + y[0], x[1] + y[1]))
for i in revenue_per_order.take(10): print(i)

#24 sortByKey
products = sc.textFile('/public/retail_db/products')

products_sortby_price = products. \
filter(lambda p: p.split(',')[4] != ""). \
map(lambda p: (float(p.split(',')[4]), p)). \
sortByKey(). \
map(lambda p: p[1])

for i in products_sortby_price.take(10): print(i)

products_sortby_price = products. \
filter(lambda p: p.split(',')[4] != ""). \
map(lambda p: ((int(p.split(',')[1]),-float(p.split(',')[4])), p)). \
sortByKey()

for i in products_sortby_price.take(10): print(i)

#get top N products by price -Global Ranking - sortByKey and Take
topN_products_sortby_price = products. \
filter(lambda p: p.split(',')[4] != ""). \
map(lambda p: (float(p.split(',')[4]), p)). \
sortByKey(False)

for i in topN_products_sortby_price. \
map(lambda p: p[1]). \
take(5): print(i)

#get top N products by price -Global Ranking - top or takeOrdered
products = sc.textFile('/public/retail_db/products')

topN_products_sortby_price = products. \
filter(lambda p: p.split(',')[4] != ""). \
top(5, key=lambda k: float(k.split(',')[4]))

for i in topN_products_sortby_price: print(i)

topN_products_sortby_price = products. \
filter(lambda p: p.split(',')[4] != ""). \
takeOrdered(5, key=lambda k: -float(k.split(',')[4]))

for i in topN_products_sortby_price: print(i)

#get top N products by price with in each category- by ranking -groupByKey 
#and flatMap

topN_products_groupby_category_id = products. \
filter(lambda p: p.split(',')[4] != ""). \
map(lambda p: (int(p.split(',')[1]), p)). \
groupByKey(). \
flatMap(lambda p: 
    sorted(p[1], key=lambda k: float(k.split(',')[4]), reverse=True)[:3]
    )

for i in topN_products_groupby_category_id.take(10): print(i)

#33
def get_topN_priced_products_per_cat_id(products_per_cat_id, topN):
    import itertools as it
    products_sorted = sorted(products_per_cat_id[1], 
                        key= lambda k: float(k.split(',')[4]), 
                        reverse=True
                        )
    product_prices = map(lambda p: float(p.split(',')[4]), products_sorted)
    topN_price = sorted(set(product_prices), reverse =True)[:topN]
    return it.takewhile(lambda p: 
                        float(p.split(',')[4]) in topN_price, 
                        products_sorted
                        )




topN_products_groupby_category_id = products. \
filter(lambda p: p.split(',')[4] != ""). \
map(lambda p: (int(p.split(',')[1]), p)). \
groupByKey()                        

for i in topN_products_groupby_category_id.take(10): print(i)

t = topN_products_groupby_category_id.filter(lambda p: p[0] == 59).first()
list(get_topN_priced_products_per_cat_id(t, 3))

topN_priced_products = products. \
filter(lambda p: p.split(',')[4] != ""). \
map(lambda p: (int(p.split(',')[1]), p)). \
groupByKey(). \
flatMap(lambda p: get_topN_priced_products_per_cat_id(p, 3))

for i in topN_priced_products.take(10): print(i)


#set operations
orders = sc.textFile('/public/retail_db/orders')
order_items = sc.textFile('/public/retail_db/order_items')

orders201312 = orders. \
filter(lambda o: o.split(',')[1][:7] == "2013-12"). \
map(lambda o: (int(o.split(',')[0]), o))

for i in orders201312.take(10): print(i)

orders201401 = orders. \
filter(lambda o: o.split(',')[1][:7] == "2014-01"). \
map(lambda o: (int(o.split(',')[0]), o))

for i in orders201401.take(10): print(i)

order_item_map = order_items. \
map(lambda oi: (int(oi.split(',')[1]),oi))

for i in order_item_map.take(10):print(i)

order_items201312 = orders201312. \
join(order_item_map). \
map(lambda oi: oi[1][1])

order_items201401 = orders201401. \
join(order_item_map). \
map(lambda oi: oi[1][1])

for i in order_items201312.take(10): print(i)
for i in order_items201401.take(10): print(i)

# set opetations union get product id sold in 2013-12 and 2014-01
products201312 = order_items201312. \
map(lambda p: int(p.split(',')[2]))

products201401 = order_items201401. \
map(lambda p: int(p.split(',')[2]))

for i in products201401.take(10):print(i)


all_products = products201312.union(products201401).distinct()

for i in all_products.collect(): print(i)

#set operation intersection -get product ids sold in both 2013-12 and 2014-01
common_products = products201312.intersection(products201401)

for i in common_products.take(10): print(i)

#set operation -minus get products ids sold in 2013-12 but not in 2014-01
products201312only = products201312. \
subtract(products201401).distinct()

for i in products201312only.take(10): print(i)

#saving as textfile
order_items = sc.textFile('/public/retail_db/order_items')

from operator import add

revenue_per_oder_id = order_items. \
map(lambda oi: (int(oi.split(',')[1]) ,float(oi.split(',')[4]))). \
reduceByKey(add). \
map(lambda r: str(r[0]) + "\t" + str(r[1]))

for i in revenue_per_oder_id.take(10): print(i)

revenue_per_oder_id.saveAsTextFile("/user/selvamsandeep/revenue_per_order_id")

for i in sc. \
textFile("/user/selvamsandeep/revenue_per_order_id"). \
take(10): print(i)

revenue_per_oder_id. \
saveAsTextFile("/user/selvamsandeep/revenue_per_order_id_compressed",
                compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")

#savin as JSON File
order_items = sc.textFile('/public/retail_db/order_items')

from operator import add

revenue_per_oder_id = order_items. \
map(lambda oi: (int(oi.split(',')[1]) ,float(oi.split(',')[4]))). \
reduceByKey(add). \
map(lambda oi: (oi[0], round(oi[1], 2)))

for i in revenue_per_oder_id.take(10): print(i)

revenue_per_oder_id_df = revenue_per_oder_id. \
toDF(schema=["order_id", "order_revenue"])

revenue_per_oder_id_df. \
save("/user/selvamsandeep/revenue_per_order_id_json2", "json")

revenue_per_oder_id_df. \
write.json("/user/selvamsandeep/revenue_per_order_id_json2")

sqlContext.read.json("/user/selvamsandeep/revenue_per_order_id_json2").show()

#45

pyspark --master yarn \
    --conf spark.ui.port=12569  \
    --num-executors 2 \
    --executor-memory 512M 

orders = sc.textFile('/public/retail_db/orders')
for i in orders.take(10): print(i)

order_items = sc.textFile('/public/retail_db/order_items')
for i in order_itmes.take(10): print(i)



for i in orders. \
map(lambda o: o.split(',')[3]).distinct(). \
collect(): print(i)

order_filtered = orders. \
filter(lambda o: o.split(',')[3] in ['COMPLETE', 'CLOSED'])
for i in order_filtered.take(10):print(i)
order_filtered.count()

orders_map = order_filtered. \
map(lambda o: (int(o.split(',')[0]), o.split(',')[1]))
for i in orders_map.take(10):print(i)

order_items_map = order_items. \
map(lambda o: 
    (int(o.split(',')[1]), (int(o.split(',')[2]), float(o.split(',')[4])))
    )
for i in order_items_map.take(10):print(i)

orders_join = orders_map.join(order_items_map)
for i in orders_join.take(10):print(i)

#47
(65536, (u'2014-05-16 00:00:00.0', (957, 299.98))) =>
((u'2014-05-16 00:00:00.0', 957), 299.98)

orders_join_map = orders_join. \
map(lambda o: ((o[1][0], o[1][1][0]), o[1][1][1]))
for i in orders_join_map.take(10):print(i)

from operator import add
daily_revenue_per_product_id = orders_join_map. \
reduceByKey(add)
for i in daily_revenue_per_product_id.take(10): print(i)

#48
products_raw =  open("/data/retail_db/products/part-00000") .\
read().splitlines()

products = sc.parallelize(products_raw)
for i in products.take(10): print(i)

products_map = products. \
map(lambda p: (int(p.split(',')[0]), p.split(',')[2]))
for i in products_map.take(10): print(i)

daily_revenue_per_product_id_map = daily_revenue_per_product_id. \
map(lambda r: (r[0][1], (r[0][0], r[1])))
for i in daily_revenue_per_product_id_map.take(10): print(i)

daily_revenue_per_product_join =daily_revenue_per_product_id_map. \
join(products_map)
for i in daily_revenue_per_product_join.take(10):print(i)

daily_revenue_per_prouct =  daily_revenue_per_product_join. \
map(lambda t: 
    ((t[1][0][0], -t[1][0][1]),  
    t[1][0][0] + "," + str(t[1][0][1]) + "," + t[1][1])
    )
for i in daily_revenue_per_prouct.take(10):print(i)

daily_revenue_per_prouct_sorted = daily_revenue_per_prouct. \
sortByKey()
for i in daily_revenue_per_prouct_sorted.take(10): print(i)

daily_revenue_per_prouct_name = daily_revenue_per_prouct_sorted. \
map(lambda r: r[1])
for i in daily_revenue_per_prouct_name.take(10): print(i)


orders = sc.textFile('/public/retail_db/orders')
order_items = sc.textFile('/public/retail_db/order_items')
order_filtered = orders. \
filter(lambda o: o.split(',')[3] in ['COMPLETE', 'CLOSED'])
orders_map = order_filtered. \
map(lambda o: (int(o.split(',')[0]), o.split(',')[1]))
order_items_map = order_items. \
map(lambda o: 
    (int(o.split(',')[1]), (int(o.split(',')[2]), float(o.split(',')[4])))
    )
orders_join = orders_map.join(order_items_map)    
orders_join_map = orders_join. \
map(lambda o: ((o[1][0], o[1][1][0]), o[1][1][1]))
from operator import add
daily_revenue_per_product_id = orders_join_map. \
reduceByKey(add)
products_raw =  open("/data/retail_db/products/part-00000") .\
read().splitlines()
products = sc.parallelize(products_raw)
products_map = products. \
map(lambda p: (int(p.split(',')[0]), p.split(',')[2]))
daily_revenue_per_product_id_map = daily_revenue_per_product_id. \
map(lambda r: (r[0][1], (r[0][0], r[1])))
daily_revenue_per_product_join =daily_revenue_per_product_id_map. \
join(products_map)
daily_revenue_per_prouct =  daily_revenue_per_product_join. \
map(lambda t: 
    ((t[1][0][0], -t[1][0][1]),  
    t[1][0][0] + "," + str(t[1][0][1]) + "," + t[1][1])
    )
daily_revenue_per_prouct_sorted = daily_revenue_per_prouct. \
sortByKey()   
daily_revenue_per_prouct_name = daily_revenue_per_prouct_sorted. \
map(lambda r: r[1])
for i in daily_revenue_per_prouct_name.take(10): print(i)

#50 saving text file

daily_revenue_per_prouct_name. \
coalesece(2). \
saveAsTextFile("/user/selvamsandeep/daily_revenue_txt_python1")

for i in sc. \
textFile("/user/selvamsandeep/daily_revenue_txt_python1"). \
take(10): print(i)

hadoop fs -ls /user/selvamsandeep/daily_revenue_txt_python1 
hadoop fs - cat /user/selvamsandeep/daily_revenue_txt_python1/part-0000 | -tail
hadoop fs - tail /user/selvamsandeep/daily_revenue_txt_python1/part-0000 

# saving in avro file
pyspark --master yarn \
    --conf spark.ui.port=12569  \
    --num-executors 2 \
    --executor-memory 512M \
    --packages com.databricks:spark-avro_2.10:2.0.1



daily_revenue_per_prouct =  daily_revenue_per_product_join. \
map(lambda t: 
    ((t[1][0][0], -t[1][0][1]),  
    (t[1][0][0], round(t[1][0][1], 2),  t[1][1]))
    )
daily_revenue_per_prouct_sorted = daily_revenue_per_prouct. \
sortByKey()   
daily_revenue_per_prouct_name = daily_revenue_per_prouct_sorted. \
map(lambda r: r[1])    

daily_revenue_per_prouct_name_df1 = daily_revenue_per_prouct_name. \
coalesce(2). \
toDF(schema=['order_date', 'revenue_per_product', 'product_name'])

daily_revenue_per_prouct_name_df1.show()

daily_revenue_per_prouct_name_df1. \
save("/user/selvamsandeep/daily_revenue_avro_python1" , 
    "com.databricks.spark.avro")
sqlContext. \
load("/user/selvamsandeep/daily_revenue_avro_python1",
    "com.databricks.spark.avro"
    ).show()

#52 
#saving file in local dir
hadoop fs -copyToLocal /user/selvamsandeep/daily_revenue_avro_python1  \
/home/selvamsandeep/daily_revenue_python/daily_revenue_avro_python1.


#53
from pyspark import SparkContext, SparkConfi
conf = SparkConf(). \
setAppName("Daily revenue per prodccut"). \
setMaster('yarn-client')

sc = SparkContext(conf=conf)

orders = sc.textFile('/public/retail_db/orders')
order_items = sc.textFile('/public/retail_db/order_items')
order_filtered = orders. \
filter(lambda o: o.split(',')[3] in ['COMPLETE', 'CLOSED'])
orders_map = order_filtered. \
map(lambda o: (int(o.split(',')[0]), o.split(',')[1]))
order_items_map = order_items. \
map(lambda o: 
    (int(o.split(',')[1]), (int(o.split(',')[2]), float(o.split(',')[4])))
    )
orders_join = orders_map.join(order_items_map)    
orders_join_map = orders_join. \
map(lambda o: ((o[1][0], o[1][1][0]), o[1][1][1]))
from operator import add
daily_revenue_per_product_id = orders_join_map. \
reduceByKey(add)
products_raw =  open("/data/retail_db/products/part-00000") .\
read().splitlines()
products = sc.parallelize(products_raw)
products_map = products. \
map(lambda p: (int(p.split(',')[0]), p.split(',')[2]))
daily_revenue_per_product_id_map = daily_revenue_per_product_id. \
map(lambda r: (r[0][1], (r[0][0], r[1])))
daily_revenue_per_product_join =daily_revenue_per_product_id_map. \
join(products_map)
daily_revenue_per_prouct =  daily_revenue_per_product_join. \
map(lambda t: 
    ((t[1][0][0], -t[1][0][1]),  
    t[1][0][0] + "," + str(t[1][0][1]) + "," + t[1][1])
    )
daily_revenue_per_prouct_sorted = daily_revenue_per_prouct. \
sortByKey()   
daily_revenue_per_prouct_name = daily_revenue_per_prouct_sorted. \
map(lambda r: r[1])

spark-submit \
`` --master yarn \
    --conf spark.ui.port=12569  \
    --num-executors 2 \
    --executor-memory 512M \
    src/main/python/daily_revenue_per_product.py

