sqlContext.sql("use selvamsandeep_retail_db_orc1")

sqlContext.sql("select * from selvamsandeep_retail_db_orc1.orders").show()

from pyspark.sql import Row
orderRdd = sc.textFile("/public/retail_db/orders")
for i in orderRdd.take(10): print(i)

ordersDF = orderRdd. \
map(lambda o: Row(
    order_id=int(o.split(",")[0]), 
    order_date=o.split(",")[1], 
    order_customer_id=int(o.split(",")[2]), 
    order_status=o.split(",")[3])).toDF()


ordersDF.show() 
ordersDF.registerTempTable("ordersDF_table")  
sqlContext.sql("select * from ordersDF_table").show()     

productsRaw = open("/data/retail_db/products/part-00000").read().splitlines()
productsRdd = sc.parallelize(productsRaw)
for i in productsRdd.take(10):print(i)

productsDF = productsRdd. \
map(lambda p: Row(
                product_id=int(p.split(",")[0]),
                product_name=p.split(",")[2] )).toDF()
productsDF.show()
productsDF.registerTempTable("products")                

#20
from pyspark.sql import Row
productsRaw = open("/data/retail_db/products/part-00000").read().splitlines()
productsRdd = sc.parallelize(productsRaw)
productsDF = productsRdd. \
map(lambda p: Row(
                product_id=int(p.split(",")[0]),
                product_name=p.split(",")[2] )).toDF()

productsDF.registerTempTable("products")

sqlContext.sql("use selvamsandeep_retail_db_txt1")