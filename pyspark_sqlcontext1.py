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

#pyspark --master yarn --conf spark.ui.port=12568 \
#  --jars /usr/share/java/mysql-connector-java.jar \
#  --driver-class-path /usr/share/jave/mysql-connector-java.jar 

from pyspark.sql.types import IntegerType, FloatType

orders_csv = spark. \
    read.csv("/media/selva/d/Big data/data/retail_db/orders"). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

orders = orders_csv. \
    withColumn('order_id', orders_csv.order_id.cast(IntegerType())). \
    withColumn('order_customer_id', orders_csv.order_customer_id.cast(IntegerType()))

orders.printSchema()
#orders.show()

order_items_csv = spark. \
    read.csv('/media/selva/d/Big data/data/retail_db/order_items'). \
    toDF('order_item_id', 'order_item_order_id', 'order_item_product_id',
         'order_item_quantity', 'order_item_subtotal', 'order_item_product_price')

order_items = order_items_csv. \
    withColumn('order_item_id', order_items_csv.order_item_id.cast(IntegerType())). \
    withColumn('order_item_order_id', order_items_csv.order_item_order_id.cast(IntegerType())). \
    withColumn('order_item_product_id', order_items_csv.order_item_product_id.cast(IntegerType())). \
    withColumn('order_item_quantity', order_items_csv.order_item_quantity.cast(IntegerType())). \
    withColumn('order_item_subtotal', order_items_csv.order_item_subtotal.cast(FloatType())). \
    withColumn('order_item_product_price', order_items_csv.order_item_product_price.cast(FloatType()))

order_items.printSchema()

orders = spark.read. \
    jdbc('jdbc:mysql://127.0.0.1:3306',
         'classicmodels.orders',
         numPartitions=4,
         properties={'user':'root','password':'suntv'})

orders.printSchema()

orderdetails = spark.read. \
    jdbc('jdbc:mysql://127.0.0.1:3306',
         'classicmodels.orderdetails',
         numPartitions=4,
         properties={'user':'root','password':'suntv'})

orderdetails.printSchema()
