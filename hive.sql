#hive launching
#problem statment
# 1. Create database user_id_retail_db_txt
create database selvamsandeep_retail_db_txt;
use selvamsandeep_retail_db_txt;	
show tables;

#from hive-site.xml
set hive.metastore.warehouse.dir;

dfs -ls /apps/hive/warehouse;

# 2. Create orders and order_items the table for retail_db
create table orders (
order_id int,
order_date string,
order_customer_id int,
order_status string
) row format delimited fields terminated by ',' stored as textfile;


show tables;

select * from orders limit 10;

load data local inpath '/data/retail_db/orders' into table orders;

dfs -ls /apps/hive/warehouse/selvamsandeep_retail_db_txt.db/orders;


create table order_items (
order_item_id int,
order_item_order_id int,
order_item_product_id int,
order_item_quantity int,
order_item_subtotal float,
order_item_product_price float
) row format delimited fields terminated by ',' 
stored as textfile;


load data local inpath '/data/retail_db/order_items' 
into table order_items;


#create database selvamsandeep_retail_db_orc sparkSQL vedio #4
create database selvamsandeep_retail_db_orc;
use selvamsandeep_retail_db_orc;	
show tables;


#created orders and order_items tables for retail_db 
#with file format as ORC
create table orders (
order_id int,
order_date string,
order_customer_id int,
order_status string
)  stored as orc;

insert into table orders select * from selvamsandeep_retail_db_txt.orders;

create table order_items (
order_item_id int,
order_item_order_id int,
order_item_product_id int,
order_item_quantity int,
order_item_subtotal float,
order_item_product_price float
) stored as orc;

insert into table order_items select * from selvamsandeep_retail_db_txt.order_items;

describe orders;

describe formatted orders;

select * from orders limit 10;


#SparkSQL  -Hive Functions - manipulating 07
use selvamsandeep_retail_db_txt;	

show functions;

create table customers (
customer_id int,
customer_fname varchar(45),
customer_lname varchar(45),
customer_email varchar(45),
customer_password varchar(45),
customer_street varchar(255),
customer_city varchar(45),
customer_state varchar(45),
customer_zipcode varchar(45)
) row format delimited fields terminated by ','
stored as textfile;

load data local inpath '/data/retail_db/customers' into table customers;

select count(1) from orders;

select sum(order_item_subtotal) from order_items;

select avg(order_item_subtotal) from order_items;

select distinct order_status from orders;

select order_status,
        case order_status
             when 'CLOSED' then 'No Action' 
             when 'COMPLETE' then 'No Action' 
             when 'ON_HOLD' then 'Pending Action'
             when 'PAYMENT_REVIEW' then 'Pending Action'
             when 'PENDING' then 'Peding Action'
             when 'PENDING_PAYMENT' then 'Pending Action'
             when 'PROCESSING' then 'Pending Action'
             else 'Risky'
        end from orders limit 10;

select order_status,
        case 
             when order_status IN ('CLOSED', 'COMPLETE') then 'No Action' 
             when order_status IN ('ON_HOLD','PAYMENT_REVIEW', 'PENDING'
                                  'PENDING_PAYMENT', 'PROCESSING')
                                   then 'Pending Action'
             else 'Risky'
        end from orders limit 10;


select nvl(order_status, 'missing') from orders limit 100;


#row level Transformations
select concat(substr(order_date, 1, 4), substr(order_date, 6, 2))
from orders limit 10;

select cast(concat(substr(order_date, 1, 4), substr(order_date, 6, 2)) as int)
from orders limit 10;

select cast(date_format(order_date, 'YYYYMM') as int) 
from orders limit 10;

select o.*, c.* from orders o, customers c 
where o.order_customer_id = c.customer_id
limit 10;


select o.*, c.* from  orders o join customers c
on o.order_customer_id = c.customer_id
limit 10;

select count(1) from orders o inner join customers c
on o.order_customer_id = c.customer_id;

select count(1) from customers c left outer join orders o 
on  o.order_customer_id = c.customer_id;

select * from customers c left outer join orders o 
on  o.order_customer_id = c.customer_id
where o.order_customer_id is null;

select count(1) from customers c left outer join orders o 
on  o.order_customer_id = c.customer_id
where o.order_customer_id is null;

select order_status, count(1) as order_count from 
orders group by order_status;


select o.order_id, o.order_date, o.order_status, 
sum(oi.order_item_subtotal) as order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 1000;

select o.order_date, round(sum(oi.order_item_subtotal),2) as daily_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_date;


select o.order_id, o.order_date, o.order_status, 
round(sum(oi.order_item_subtotal), 2) as order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 1000
order by o.order_date, order_revenue desc;

select o.order_id, o.order_date, o.order_status, 
round(sum(oi.order_item_subtotal), 2) as order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 1000
distribute by o.order_date sort by o.order_date, order_revenue desc;
