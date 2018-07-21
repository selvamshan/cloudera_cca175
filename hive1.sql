

create database selvamsandeep_retail_db_txt1;
use selvamsandeep_retail_db_txt1;

create table orders (
    order_id int,
    order_date string,
    order_customer_id int,
    order_status string
) row format delimited fields terminated by ',';


load data local inpath '/data/retail_db/orders' into table orders;

select * from orders limit 10;

create table customers (
    customer_id int,
    customer_fname varchar(45),
    customer_lname varchar(45),
    customer_email varchar(45),
    customer_password varchar(45),
    customer_street varchar(45),
    csutomer_city varchar(45),
    customer_state varchar(45),
    customer_zipcode varchar(45)
) row format delimited fields terminated by ',' stored as textfile;

load data local inpath '/data/retail_db/customers' into table customers;

select * from customers limit 10;

dfs -ls /apps/hive/warehouse/selvamsandeep_retail_db_txt1.db/customers;


create table order_items (
    order_item_id int,
    order_item_order_id int,
    order_item_product_id int,
    order_item_quatity int,
    order_item_subtotal float,
    order_item_product_price float
) row format delimited fields terminated by ',';

load data local inpath '/data/retail_db/order_items' into table order_items;

select * from order_items limit 10;


create database selvamsandeep_retail_db_orc1;
use selvamsandeep_retail_db_orc1;

create table orders (
    order_id int,
    order_date string,
    order_customer_id int,
    order_status string
) stored as orc;

insert into table orders select * from selvamsandeep_retail_db_txt1.orders;

create table order_items (
    order_item_id int,
    order_item_order_id int,
    order_item_product_id int,
    order_item_quatity int,
    order_item_subtotal float,
    order_item_product_price float
) stored as orc;

insert into table order_items 
select * from selvamsandeep_retail_db_txt1.order_items;




pyspark  --master yarn --conf spark.ui.port=12548

sqlContext.sql("use selvamsandeep_retail_db_txt1")

sqlContext.sql("show tables").show()

sqlContext.sql("describe formatted orders").show()

for i in sqlContext.sql("describe formatted orders").collect():print(i)

sqlContext.sql("select * from orders limit 10").show()

#in hive
use selvamsandeep_retail_db_txt1;

select order_status, length(order_status) from orders limit 10;

#07
#08
select TO_DATE('2000-01-01 10:20:30');

select  order_date, to_date(order_date), year(to_date(order_date)) 
from orders   limit 10;

#10
select order_status,
case order_status         
            when 'CLOSED' then 'No Action'
            when 'COMPLETE' then 'No Action'  
            when 'ON_HOLD' then 'Pending action' 
            when 'PAYMENT_REVIEW' then 'Pending action' 
            when 'PENDING' then 'Pending action'  
            when 'PENDING_PAYMENT' then 'Pending action' 
            when 'PROCESSING' then 'Pending action'
            else 'Risky' end
            from orders limit 50;

select order_status,
case         
    when order_status in ('CLOSED', 'COMPLETE') then 'No Action'  
    when order_status ('ON_HOLD', 'PAYMENT_REVIEW', 'PENDING','PENDING_PAYMENT', 
    'PROCESSING') then 'Pending action'
    else 'Risky' end
    from orders limit 50; 

 #11              
select concat(substr(order_date, 1, 4), substr(order_date, 6, 2))
        from orders limit 10;

select cast(concat(substr(order_date, 1, 4), substr(order_date, 6, 2)) as int)
        from orders limit 10;  

select cast(date_format(order_date, 'YYYYMM') as int)
        from orders limit 10; 

#12                            
select o.*, c.* from orders o, customers c 
where o.order_customer_id = c.customer_id
limit 10;

select o.*, c.* from orders o join customers c 
on o.order_customer_id = c.customer_id
limit 10;

select count(1) from orders o inner join customers c 
on o.order_customer_id = c.customer_id;

select count(1) from orders o left outer join customers c 
on o.order_customer_id = c.customer_id;

select count(1) from orders o left outer join customers c 
on o.order_customer_id = c.customer_id
where o.order_customer_id is null;

select * from orders o left outer join customers c 
on o.order_customer_id = c.customer_id
where o.order_customer_id is null;

select * from customers 
where customer_id not in(select distinct order_customer_id from orders);

#13
select order_status, count(1) from orders group by order_status;

select o.order_id, o.order_date, o.order_status, sum(oi.order_item_subtotal) order_revenue
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status 
having sum(oi.order_item_subtotal) >= 1000;


select  o.order_date, round(sum(oi.order_item_subtotal), 2) daily_revenue
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_date;


select o.order_id, o.order_date, o.order_status, sum(oi.order_item_subtotal) order_revenue
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status 
having sum(oi.order_item_subtotal) >= 1000
order by o.order_date, order_revenue desc;

select o.order_id, o.order_date, o.order_status, sum(oi.order_item_subtotal) order_revenue
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status 
having sum(oi.order_item_subtotal) >= 1000
distribute by o.order_date sort by o.order_date, order_revenue desc;

use selvamsandeep_retail_db_txt1;
#16
#sum
select * from (
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id),2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id),2) pct_revenue
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')) q
where order_revenue  >= 1000
order by order_date, order_revenue desc;

#sum avg
select * from (
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal)  
         over (partition by o.order_id),2) order_revenue,
round(oi.order_item_subtotal/(sum(oi.order_item_subtotal) 
         over (partition by o.order_id)), 2) pct_revenue,
round(avg(oi.order_item_subtotal) 
         over (partition by o.order_id),2) avg_revenue
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')) q
where order_revenue  >= 1000
order by order_date, order_revenue desc;


#17
#rank
select * from (
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id),2) order_revenue,
rank() over (partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
dense_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
percent_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) pct_rnk_revenue,
row_number() over (partition by o.order_id order by oi.order_item_subtotal desc) rn_orderby_revenue,
row_number() over (partition by o.order_id) rn_revenue
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')) q
where order_revenue  >= 1000
order by order_date, order_revenue desc, rnk_revenue;


#18
select * from (
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id),2) order_revenue,
lead(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) lead_order_item_subtotal,
lag(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) lag_order_item_subtotal,
first_value(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) first_order_item_subtotal,
last_value(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) last_order_item_subtotal
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')) q
where order_revenue  >= 1000
order by order_date, order_revenue desc;


#19 
 use selvamsandeep_retail_db_orc;


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