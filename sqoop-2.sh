sqoop list-databases \
    --connect jdbc:mysql://ms.itversity.com:3306 \
    --username retail_user \
    --password itversity



sqoop list-tables \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db  \
    --username retail_user \
    --password itversity  


sqoop eval \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --query "SELECT * FROM orders LIMIT 10"


sqoop eval \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_export \
    --username retail_user \
    --password itversity \
    --query "CREATE TABLE dummy (i INT)"    

sqoop eval \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_export \
    --username retail_user \
    --password itversity \
    --query "INSERT INTO dummy VALUES (1)"    

 sqoop eval \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_export \
    --username retail_user \
    --password itversity \
    --query "SELECT * FROM dummy"   


sqoop eval \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --e "SELECT * FROM order_items LIMIT 10"    

sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items \
    --warehouse-dir /user/selvamsandeep/sqoop_import/retail_db

sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items \
    --warehouse-dir /user/selvamsandeep/sqoop_import/retail_db \
    --num-mappers 1 \
    --delete-target-dir 


sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items \
    --warehouse-dir /user/selvamsandeep/sqoop_import/retail_db \
    --num-mappers 1 \
    --append

 sqoop import \
    -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table orders \
    --warehouse-dir /user/selvamsandeep/sqoop_import/retail_db \
    --split-by order_status  

 #for with out primery key split by column parameter
 sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items_nopk \
    --warehouse-dir /user/selvamsandeep/retail_db \
    --split-by order_item_id  


# for with out primery key autoreset to one maper
sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items_nopk \
    --delete-target-dir \
    --warehouse-dir /user/selvamsandeep/retail_db \
    --autoreset-to-one-mapper 


# saving file in diffeent format

sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items \
    --delete-target-dir \
    --warehouse-dir /user/selvamsandeep/retail_db \
    --num-mappers 2 \
    --as-sequencefile 

sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items \
    --delete-target-dir \
    --warehouse-dir /user/selvamsandeep/retail_db \
    --num-mappers 2 \
    --as-textfile \
    --compress \
    --compression-codec org.apache.hadoop.io.compress.SnappyCodec


#boundry query
sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items \
    --delete-target-dir \
    --warehouse-dir /user/selvamsandeep/retail_db \
    --boundary-query "SELECT min(order_item_id), max(order_item_id) 
    FROM order_items WHERE order_item_id > 99999 "

sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items \
    --delete-target-dir \
    --warehouse-dir /user/selvamsandeep/retail_db \
    --boundary-query "SELECT 100000, 172198"

sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items \
    --delete-target-dir \
    --columns order_item_order_id,order_item_id,order_item_subtotal \
    --warehouse-dir /user/selvamsandeep/sqoop_import/retail_db \
    -m 2

 mysql -u retail_user -h ms.itversity.com -p
 use retail_db;

 SELECT o.*, sum(oi.order_item_subtotal) order_revenue
 FROM orders o JOIN order_items oi 
 ON o.order_id = oi.order_item_order_id 
 GROUP BY o.order_id, o.order_date, o.order_customer_id, o.order_status;

 sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --delete-target-dir \
    --target-dir /user/selvamsandeep/sqoop_import/retail_db/orders_with_revenue \
    -m 2 \
    --query "SELECT o.*, sum(oi.order_item_subtotal) order_revenue 
    FROM orders o JOIN order_items oi 
    ON o.order_id = oi.order_item_order_id and \$CONDITIONS
    GROUP BY o.order_id, o.order_date, o.order_customer_id, o.order_status" \
    --split-by order_id 


mysql -u hr_user -h ms.itversity.com -p itversity

#filling null value and using delimeter
sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/hr_db \
    --username hr_user \
    --password itversity \
    --table employees \
    --delete-target-dir \
    --warehouse-dir /user/selvamsandeep/sqoop_import/hr_db  \
    --null-non-string -1 \
    --fields-terminated-by "\t" \
    --line-terminated-by ":"



sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/hr_db \
    --username hr_user \
    --password itversity \
    --table employees \
    --delete-target-dir \
    --warehouse-dir /user/selvamsandeep/sqoop_import/hr_db  \
    --null-non-string -1 \
    --fields-terminated-by "\000" \
    --lines-terminated-by ":"


 # importing partial data 
 sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --delete-target-dir  \
    --target-dir /user/selvamsandeep/sqoop_import/retail_db/orders \
    --num-mappers 2 \
    --query "SELECT * FROM orders WHERE \$CONDITIONS and order_date like '2013-%'"  \
    --split-by order_id  

sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --target-dir /user/selvamsandeep/sqoop_import/retail_db/orders \
    --num-mappers 2 \
    --table orders \
    --where "order_date like '2014-02%'" \
    --append 


sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --target-dir /user/selvamsandeep/sqoop_import/retail_db/orders \
    --num-mappers 2 \
    --table orders \
    --check-column order_date \
    --incremental append \
    --last-value '2014-02-28'

#sqoop import to hive table
CREATE database selvamsandeep_sqoop_import;

USE selvamsandeep_sqoop_import;

CREATE TABLE t (i int);

INSERT INTO TABLE t VALUES (1);

SELECT * FROM t;

DROP TABLE t;


sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items \
    --hive-import \
    --hive-database selvamsandeep_sqoop_import \
    --hive-table order_items \
    --num-mappers 2


    USE selvamsandeep_sqoop_import;

    describe formatted order_items;

hadoop fs -ls \
   	hdfs://nn01.itversity.com:8020/apps/hive/warehouse/selvamsandeep_sqoop_import.db/order_items	 

hadoop fs -get \
    hdfs://nn01.itversity.com:8020/apps/hive/warehouse/selvamsandeep_sqoop_import.db/order_items \
    selvamsandeep_sqoop_import_hive_order_items

sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items \
    --hive-import \
    --hive-database selvamsandeep_sqoop_import \
    --hive-table order_items \
    --hive-overwrite \
    --num-mappers 2



 sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table orders \
    --hive-import \
    --hive-database selvamsandeep_sqoop_import \
    --hive-table orders \
    --hive-overwrite \
    --num-mappers 2   


sqoop import \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --table order_items \
    --hive-import \
    --hive-database selvamsandeep_sqoop_import \
    --hive-table order_items \
    --create-hive-table \
    --num-mappers 2



sqoop import-all-tables \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user \
    --password itversity \
    --warehouse-dir /user/selvamsandeep/sqoop_import/retail_db \
    --autoreset-to-one-mapper  


CREATE TABLE daily_revenue AS
SELECT o.order_date, sum(oi.order_item_subtotal) daily_revenue
FROM orders o JOIN order_items oi 
ON o.order_id = oi.order_item_order_id 
WHERE order_date LIKE '2013-07%'
GROUP BY order_date;

CREATE TABLE daily_revenue_selva (
    order_date varchar(30),    
    revenue float,    
);

sqoop export \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_export \
    --username  retail_user \
    --password itversity \
    --export-dir /apps/hive/warehouse/selvamsandeep_sqoop_import.db/daily_revenue \
    --table daily_revenue_selva \
    --input-fields-terminated-by "\001"


CREATE TABLE daily_revenue_selva_2 (
    revenue float,
    order_date varchar(30) primary key,
    description varchar(200)
);
   
 sqoop export \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_export \
    --username  retail_user \
    --password itversity \
    --export-dir /apps/hive/warehouse/selvamsandeep_sqoop_import.db/daily_revenue \
    --table daily_revenue_selva2 \
    --columns order_date,revneue \
    --input-fields-terminated-by "\001" \    
    --num-mappers 2

 #update and uosetmetger 

CREATE TABLE daily_revenue_selva (
    order_date varchar(30) primary key,   
    revenue float    
);

sqoop export \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_export \
    --username  retail_user \
    --password itversity \
    --export-dir /apps/hive/warehouse/selvamsandeep_sqoop_import.db/daily_revenue \
    --table daily_revenue_selva \
    --input-fields-terminated-by "\001" \
    
    
 USE selvamsandeep_sqoop_import;

INSERT INTO TABLE daily_revenue
SELECT o.order_date, sum(oi.order_item_subtotal) daily_revenue
FROM orders o JOIN order_items oi 
ON o.order_id = oi.order_item_order_id 
WHERE order_date LIKE '2013-08%'
GROUP BY order_date;

#in mysql
UPDATE daily_reveny SET revenue = 0;
  
sqoop export \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_export \
    --username  retail_user \
    --password itversity \
    --export-dir /apps/hive/warehouse/selvamsandeep_sqoop_import.db/daily_revenue \
    --table daily_revenue_selva \
    --update-key order_date \
    --update-mode allowinsert \
    --input-fields-terminated-by "\001" \
    --num-mappers 1

TRUNCATE TABLE daily_revenue;

INSERT INTO TABLE daily_revenue
SELECT o.order_date, sum(oi.order_item_subtotal) daily_revenue
FROM orders o JOIN order_items oi 
ON o.order_id = oi.order_item_order_id 
GROUP BY order_date;    
      

sqoop export \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_export \
    --username  retail_user \
    --password itversity \
    --export-dir /apps/hive/warehouse/selvamsandeep_sqoop_import.db/daily_revenue \
    --table daily_revenue_selva \
    --input-fields-terminated-by "\001" 


 
CREATE TABLE daily_revenue_selva_stage(
    order_date varchar(30) primary key,   
    revenue float    
);
   
sqoop export \
    --connect jdbc:mysql://ms.itversity.com:3306/retail_export \
    --username  retail_user \
    --password itversity \
    --export-dir /apps/hive/warehouse/selvamsandeep_sqoop_import.db/daily_revenue \
    --table daily_revenue_selva \
    --staging-table daily_revenue_selva_stage \
    --clear-staging-table \
    --input-fields-terminated-by "\001" 

    

