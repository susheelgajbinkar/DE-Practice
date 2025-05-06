# Databricks notebook source
# DBTITLE 1,yy
# Show the students whose total of two highest marks that are atleast 160

df = spark.createDataFrame([('A','X',80), 
                            ('A','Y',70),
                            ('A','Y',75),
                            ('B','X',90),
                            ('B','Y',91),
                            ('B','Z',75),
                            ('C','X',60),
                            ('C','Y',93),
                            ('C','Z',81)], ['StudentID', 'SubjectID','MarksObtained'])

#df.show()

from pyspark.sql.functions import *
from pyspark.sql.window import Window

df_win = Window.partitionBy("StudentID").orderBy(col("MarksObtained").desc())

df_students = df.withColumn("rnk", dense_rank().over(df_win)).filter("rnk<=2")
df_students_grouped = df_students.groupBy("StudentID").agg(sum("MarksObtained").alias("marks_sum")).where("marks_sum>=160")
df_students_grouped.show()

df.createOrReplaceTempView("students")

df_sql = spark.sql("select StudentID, sum(MarksObtained) as marks_sum from (select *,\
                   dense_rank() over (partition by StudentID order by MarksObtained desc) rnk from students) where rnk<=2 group by StudentID having sum(MarksObtained)>=160 ")
df_sql.show()



# COMMAND ----------

# Write in pyspark code to seperate the subjects in seperate rows for each student

df1ms = spark.createDataFrame([("abc", "maths,physics,chemistry"), ("abcd", "maths,bio")], ["sname", "abcd"])

df1ms_final = df1ms.withColumn("abcd_splitted", split("abcd",",")).withColumn("subjects", explode("abcd_splitted")).drop("abcd", "abcd_splitted")
df1ms_final.show()

df1ms.createOrReplaceTempView("subjects")

df_subjects_sql = spark.sql("select sname, explode(split(abcd,',')) as subjects from subjects")
df_subjects_sql.show()

# COMMAND ----------

# How to handle corrupt or bad record in Apache Spark

from pyspark.sql.types import *

schema = StructType([(StructField("ID", IntegerType(), True),
                      StructField("name", StringType(), True),
                      StructField("age", IntegerType(), True),
                      StructField("_corrupt_record", StringType(), True))])


df = spark.read.option("mode", "PERMISSIVE").schema(schema).option("header", True)\
    .option("ColumnNameOfCorruptRecord", "_corrupt_record").csv("/customers.csv").cache()

# COMMAND ----------

# Write a query to find second highest salary of employee from bottom

df2 = spark.createDataFrame([("john",24000), ("david",55000), ("rob",55000), ("elon",75000)], ["employee_name","salary"])

df2_win = Window.orderBy(col("salary").desc())
df2_emp = df2.withColumn("rnk", rank().over(df2_win)).filter("rnk==2").drop("rnk")
df2_emp.show()

df2.createOrReplaceTempView("employees")
df2_sql = spark.sql("select employee_name, salary from (select *, rank() over (order by salary desc) rnk from employees) where rnk==2")
df2_sql.show()

# COMMAND ----------

# Implement a soln to automatically assign ages where the eldest adult in the family is paired with the youngest child, ensuring proper guardianship within the family structure.

data = [("John", "Adult", 35),
("Alice", "Adult", 30),
("Bob", "Child", 10),
("Charlie", "Child", 8),
("ben", "Child", 12),
("David", "Adult", 40)]

df = spark.createDataFrame(data, ["person", "type", "age"])
df_win_adult = Window.partitionBy("type").orderBy(col("age").desc())
df_win_child = Window.partitionBy("type").orderBy("age")

df_adult = df.withColumn("rn_adult", row_number().over(df_win_adult)).filter("type=='Adult'")
df_child = df.withColumn("rn_child", row_number().over(df_win_child)).filter("type=='Child'")
df_joined = df_adult.join(df_child, df_child.rn_child==df_adult.rn_adult, how="inner").drop("rn_adult", "rn_child")
df_joined.show()

df.createOrReplaceTempView("persons")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with cte_adult as (
# MAGIC select *,
# MAGIC row_number() over (partition by type order by age desc) rn_adult from persons where type="Adult"),
# MAGIC
# MAGIC cte_child as (
# MAGIC select *,
# MAGIC row_number() over (partition by type order by age) rn_child from persons where type="Child")
# MAGIC
# MAGIC select A.person, A.type, A.age, C.person, C.type, C.age from cte_adult A inner join cte_child C on A.rn_adult==C.rn_child;

# COMMAND ----------

# pivoting table example

from pyspark.sql.functions import *
data = [("A", "X", 10), ("A", "Y", 20), ("B", "X", 30), ("B", "Y", 40)]
df = spark.createDataFrame(data, ["Category", "Type", "Value"])
#df.show()

df_pivoted = df.groupBy("Category").pivot("type").agg(sum("Value"))
df_pivoted.show()

df.createOrReplaceTempView("pivot_ex1")

df_sql = spark.sql("select * from pivot_ex1 pivot (sum(Value) for type in ('X','Y'))")
df_sql.show()


# COMMAND ----------

# pivoting table example2

data=[
('Rudra','math',79),
('Rudra','eng',60),
('Shivu','math', 68),
('Shivu','eng', 59),
('Anu','math', 65),
('Anu','eng',80)
]
schema="Name string, Sub string, Marks int"
df = spark.createDataFrame(data, schema)

df_pivoted = df.groupBy("name").pivot("sub").agg(sum("marks"))
df_pivoted.show()

df.createOrReplaceTempView("pivot_ex2")

df_sql = spark.sql("select * from pivot_ex2 pivot (sum(marks) for sub in ('eng', 'math'))")
df_sql.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE flights6 
# MAGIC (
# MAGIC     cid VARCHAR(512),
# MAGIC     fid VARCHAR(512),
# MAGIC     origin VARCHAR(512),
# MAGIC     Destination VARCHAR(512)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO flights6 (cid, fid, origin, Destination) VALUES ('1', 'f1', 'Del', 'Hyd');
# MAGIC INSERT INTO flights6 (cid, fid, origin, Destination) VALUES ('1', 'f2', 'Hyd', 'Blr');
# MAGIC INSERT INTO flights6 (cid, fid, origin, Destination) VALUES ('2', 'f3', 'Mum', 'Agra');
# MAGIC INSERT INTO flights6 (cid, fid, origin, Destination) VALUES ('2', 'f4', 'Agra', 'Kol');

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.sql("select * from flights6")
df_joined = df.alias("df1").join(df.alias("df2"), col("df1.origin")==col("df2.Destination"), "inner")
df_joined.select(col("df1.cid"), col("df2.origin"), col("df1.destination")).orderBy(col("df1.cid")).show()


-----pending(sql)--------

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE sales6
# MAGIC (
# MAGIC     order_date date,
# MAGIC     customer VARCHAR(512),
# MAGIC     qty INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO sales6 (order_date, customer, qty) VALUES ('2021-01-01', 'C1', '20');
# MAGIC INSERT INTO sales6 (order_date, customer, qty) VALUES ('2021-01-01', 'C2', '30');
# MAGIC INSERT INTO sales6 (order_date, customer, qty) VALUES ('2021-02-01', 'C1', '10');
# MAGIC INSERT INTO sales6 (order_date, customer, qty) VALUES ('2021-02-01', 'C3', '15');
# MAGIC INSERT INTO sales6 (order_date, customer, qty) VALUES ('2021-03-01', 'C5', '19');
# MAGIC INSERT INTO sales6 (order_date, customer, qty) VALUES ('2021-03-01', 'C4', '10');
# MAGIC INSERT INTO sales6 (order_date, customer, qty) VALUES ('2021-04-01', 'C3', '13');
# MAGIC INSERT INTO sales6 (order_date, customer, qty) VALUES ('2021-04-01', 'C5', '15');
# MAGIC INSERT INTO sales6 (order_date, customer, qty) VALUES ('2021-04-01', 'C6', '10');
# MAGIC
# MAGIC --Question :Find the new customer added in each month

# COMMAND ----------

df = spark.sql("select * from sales6")

from pyspark.sql.functions import *
from pyspark.sql.window import Window

df_win = Window.partitionBy(col("customer")).orderBy(col("order_date"))

df_sales6 = df.withColumn("rn",row_number().over(df_win)).filter("rn==1")
df_sales6.groupBy(col("order_date")).agg(countDistinct(col("customer"))).show()


--------pending(sql)-----------

# COMMAND ----------

"""Input Table:
+-------+----------+------+
|product|sales_date|amount|
+-------+----------+------+
| TV|2016-11-27| 800|
| TV|2016-11-30| 900|
| TV|2016-12-29| 500|
| TV|2017-01-20| 400|
| FRIDGE|2016-10-11| 760|
| FRIDGE|2016-10-13| 400|
+-------+----------+------+

Expected Output:
-------+----------+-----------------+------------------+----------------------+
|product|sales_date|current_day_sales|next_2nd_day_sales|previous_2nd_day_sales|
+-------+----------+-----------------+------------------+----------------------+
| TV|2016-11-27| 800| 500| null|
| TV|2016-11-30| 900| 400| null|
| TV|2016-12-29| 500| null| 800|
| TV|2017-01-20| 400| null| 900|
| FRIDGE|2016-10-11| 760| null| null|
| FRIDGE|2016-10-13| 400| null| null|
+-------+----------+-----------------+------------------+----------------------+
"""

data = [('TV', '2016-11-27', 800),
 ('TV', '2016-11-30', 900),
 ('TV', '2016-12-29', 500),
 ('TV', '2017-01-20', 400),
 ('FRIDGE','2016-10-11', 760),
 ('FRIDGE', '2016-10-13', 400)]

schema = 'product string, sales_date string, amount long'

df = spark.createDataFrame(data, schema)

df_win =  Window.partitionBy("product").orderBy(to_date(col("sales_date"), "yyyy-MM-dd"))

df_products = df.withColumn("next_2nd_day_sales", lead(col("amount"), 2).over(df_win)).withColumn("previous_2nd_day_sales", lag(col("amount"), 2).over(df_win)).withColumnRenamed("amount", "current_day_sales")
df_products.show()

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product, sales_date, amount as current_day_sales,
# MAGIC lead(amount, 2) over (partition by product order by sales_date) next_2nd_day_sales,
# MAGIC lag(amount, 2) over (partition by product order by sales_date) previous_2nd_day_sales from products;
# MAGIC

# COMMAND ----------

#calculate month-wise running sum of revenue as shown in the resulatnt table. 

from pyspark.sql.types import *
from pyspark.sql import Row
data = [
    Row(revenue=3000, date='22-may'),
    Row(revenue=5000, date='23-may'),
    Row(revenue=5000, date='25-may'),
    Row(revenue=10000, date='22-june'),
    Row(revenue=1250, date='03-july')
]

schema = StructType([
    StructField("revenue", IntegerType(), True),
    StructField("date", StringType(), True) 
])

df = spark.createDataFrame(data, schema)

df = df.withColumn("date", to_date("date", "dd-MMMM"))
df_win = Window.partitionBy(date_format("date", "MMMM")).orderBy("date")
df_rev = df.withColumn("running_sum", sum("revenue").over(df_win)).withColumn("month", date_format("date", "MMMM")).drop("date", "revenue")
df_rev.show()

df.createOrReplaceTempView("revenues")

df_sql = spark.sql("select running_sum, date_format(date, 'MMMM') as month from (select *,\
    sum(revenue) over (partition by date_format(to_date(date, 'dd-MMMM'),'MMMM') order by date) running_sum from revenues)")
df_sql.show()

# COMMAND ----------

'''✅ Given below dataframe, (df [uniqueid: string, status_value(status:code:codetype): array<string>])

uniqueid           status_value(status:code:codetype)

20d75c97-5fee-11e8-92c7-67fe1c388607 ['A:X:M', 'B:Y:N', 'C:Z:O', 'D:W:P', 'E:V:Q','A:W:P']
20d75c98-5fee-11e8-92c7-5f0316c1a74f ['A:X:M', 'B:W:N', 'C:L:O'] 
20d75c99-5fee-11e8-92c7-d9bfa897a151 ['A:X:M', 'F:Y:N', 'H:Z:O','A:W:P']

For status in A, B, C output should be as follows. Write code in PySpark.

uniqueid     status_value(status:code:codetype)

20d75c97-5fee-11e8-92c7-67fe1c388607 ['A:X:M','B:Y:N', 'C:Z:O','A:W:P']
20d75c98-5fee-11e8-92c7-5f0316c1a74f ['A:X:M', 'B:W:N', 'C:L:O']
20d75c99-5fee-11e8-92c7-d9bfa897a151 ['A:X:M','A:W:P']'''

data=[
("20d75c97-5fee-11e8-92c7-67fe1c388607", ['A:X:M', 'B:Y:N', 'C:Z:O', 'D:W:P', 'E:V:Q','A:W:P']),
("20d75c98-5fee-11e8-92c7-5f0316c1a74f", ['A:X:M', 'B:W:N', 'C:L:O']), 
("20d75c99-5fee-11e8-92c7-d9bfa897a151", ['A:X:M', 'F:Y:N', 'H:Z:O','A:W:P'])]

schema = ("uniqueid", "status_value")

df = spark.createDataFrame(data, schema)
df = df.withColumn("status_exploded", explode(col("status_value"))).filter(col("status_exploded").rlike("^[A-C]"))
df_final = df.groupBy(col("uniqueid")).agg(collect_list(col("status_exploded")))\
    .withColumnRenamed("collect_list(status_exploded)", "status_vlaue")
display(df_final)


--------pending(sql)----------

# COMMAND ----------

"""✅ Assume you have an events table on app analytics. Write a SQL query to get the app’s click-through rate (CTR %) in 2022. Output the results in percentages rounded to 2 decimal places.

Notes:

Percentage of click-through rate = 100.0 * Number of clicks / Number of impressions
To avoid integer division, you should multiply the click-through rate by 100.0, not 100.

events 

Table:
Column Name Type
app_id integer
event_type string
timestamp datetime

Example Input:
app_id event_type timestamp
123 impression 07/18/2022 11:36:12
123 impression 07/18/2022 11:37:12
123 click 07/18/2022 11:37:42
234 impression 07/18/2022 14:15:12
234 click 07/18/2022 14:16:12

Example Output:
app_id ctr
123 50.00
234 100.00"""

data = ([(123, "impression", "07/18/2022 11:36:12"),
(123, "impression", "07/18/2022 11:37:12"),
(123, "click", "07/18/2022 11:37:42"),
(234, "impression", "07/18/2022 14:15:12"),
(234, "click", "07/18/2022 14:16:12")])

schema = "app_id int, event_type string, timestamp string"

df = spark.createDataFrame(data, schema)

df_win = Window.partitionBy(col("app_id"))

df_events = df.withColumn("count_imp", count(when(col("event_type")=="impression", True)).over(df_win)).withColumn("count_cli", count(when(col("event_type")=="click", True)).over(df_win))

df_events.withColumn("ctr%", (col("count_cli")/col("count_imp"))*100).drop("event_type", "timestamp", "count_imp", "count_cli").dropDuplicates().show()


-----------pending(sql)------



# COMMAND ----------

"""✅ Find duplicates in the following array."""

arr1 = [71,78,76,74,73,71,72,75]

df = spark.createDataFrame([(num,) for num in arr1], ["number"])
df_dup = df.groupBy(col("number")).agg(count(col("number")).alias("count"))
df_dup.filter(col("count")>1).show()


---------pending(sql)----------

# COMMAND ----------

import pyspark.sql.functions as F
data = [
    (1, "Gaurav", ["Pune", "Banglore", "Hyderabad"]),
    (2, "Rishab", ["Mumbai", "Banglore", "Pune"])
]
parallelized_data = sc.parallelize(data)                    #to create rdd
df = parallelized_data.toDF(("EmpID","Name","Location"))

df_exploded = df.withColumn("location_exploded", explode(col("location"))).drop(col("location"))
df_exploded.show()

df.createOrReplaceTempView("locations")

df_sql = spark.sql("select empid, name, explode(location) as location_exploded from locations")
df_sql.show()

# COMMAND ----------

#find min, max and cum_salaries for each dept

data = [("James", "Sales", 2000),
("sofy", "Sales", 3000),
("Laren", "Sales", 4000),
("Kiku", "Sales", 5000),
("Sam", "Finance", 6000),
("Samuel", "Finance", 7000),
("Mausam", "Marketing", 12000),
("Lamba", "Marketing", 13000),
("Jogesh", "HR", 14000),
("Mannu", "HR", 15000)
]

schema = "name string, dept string, salary int"

df = spark.createDataFrame(data, schema)

from pyspark.sql.functions import *
from pyspark.sql.window import Window

df_win = Window.partitionBy(col("dept")).orderBy("salary")

df_final = df.withColumn("min_sal", min(col("salary")).over(df_win))\
    .withColumn("max_salary", max(col("salary")).over(df_win))\
        .withColumn("cum_sal", sum(col("salary")).over(df_win))
df_final.show()


--------pending(sql)-----------

# COMMAND ----------

#count of movies for each genre

import pyspark.sql.functions as E

df = spark.createDataFrame([('The Shawshank Redemption',['Drama', 'Crime']),
                  ('The Godfather', ['Drama', 'Crime']),
                  ('Pulp Fiction', ['Drama', 'Crime','Thriller']),
                  ('The Dark Knight', ['Drama', 'Crime','Thriller','Action']),
                  ],["name", "genres"])

df_final = df.withColumn("genre_exp", explode(col("genres"))).groupBy(col("genre_exp"))\
    .agg(count(col("name")).alias("movie_count")).drop(col("genres"))
df_final.show()


-----------pending(sql)-----------

# COMMAND ----------

# Write a Pyspark code to find the output table as given below- employeeid,default_number,total_entry,total_login, total_logout, latest_login,latest_logout.

checkin_df = spark.createDataFrame([(1000, 'login', '2023-06-16 01:00:15.34'),
                                   (1000, 'login', '2023-06-16 02:00:15.34'),
                                   (1000, 'login', '2023-06-16 03:00:15.34'),
                                   (1000, 'logout', '2023-06-16 12:00:15.34'),
                                   (1001, 'login', '2023-06-16 01:00:15.34'),
                                   (1001, 'login', '2023-06-16 02:00:15.34'),
                                   (1001, 'login', '2023-06-16 03:00:15.34'),
                                   (1001, 'logout', '2023-06-16 12:00:15.34')],
                                  ["employeeid", "entry_details", "timestamp_details"])

detail_df = spark.createDataFrame([(1001, 9999, 'false'),
                                   (1001, 1111, 'false'),
                                   (1001, 2222, 'true'),
                                   (1003, 3333, 'false')],
                                  ["id", "phone_number", "isdefault"])

joined_df = checkin_df.join(detail_df, checkin_df.employeeid==detail_df.id, "inner")

#joined_df_grouped = joined_df.filter(col("isdefault")=="true")
joined_df_final = joined_df.filter(col("isdefault")=="true").alias("default_number")\
    .groupBy("employeeid").agg(countDistinct(col("timestamp_details")).alias("total_entry")\
        , count(when(col("entry_details")=="login", True)).alias("total_login")\
            , count(when(col("entry_details")=="logout", True)).alias("total_logout")\
                , max(when(col("entry_details")=="login", col("timestamp_details"))).alias("latest_login")\
                    , max(when(col("entry_details")=="logout", col("timestamp_details"))).alias("latest_logout"))

df_finall = joined_df_final.select("employeeid", "total_entry", "total_login", "total_logout", "latest_login", "latest_logout")
display(df_finall)

-----------pending(sql)---------

# COMMAND ----------

"""Q: Given a DataFrame df with columns id, name, and sales.
1: Filter out records where sales are less than 50.
2: Create a new column sales_category which categorize sales into ‘Low’ (50–100), ‘Medium’ (101–200), and ‘High’ (greater than 200).
3: Group the data by sales_category and calculate the average sales for each category."""

# Sample data
df_data = [
    (1, 'Alice', 45),
    (2, 'Bob', 120),
    (3, 'Charlie', 75),
    (4, 'David', 180),
    (5, 'Eve', 220)
]
# Column names for the DataFrame
df_columns = ["id", "name", "sales"]

df = spark.createDataFrame(df_data, df_columns)

from pyspark.sql.functions import *

#1
df_50 = df.filter(col("sales")<50)
df_50.show()

#2
df_category = df.withColumn("sales_category", when((col("sales") >= 50) & (col("sales") <= 100), "Low").when((col("sales") >= 101) & (col("sales") <= 200), "Medium").otherwise("High"))
df_category.show()

#3
df_grouped = df_category.groupBy(col("sales_category")).agg(avg(col("sales")))
df_grouped.show()


------pending(sql)---------

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

data = [
 (1, "Amit", "HR", 70000, "15-01-2019"),
 (2, "Rajesh", "IT", 80000, "22-03-2018"),
 (3, "Neeta", "IT", 85000, "30-07-2017"),
 (4, "Anjali", "Sales", 75000, "05-11-2020"),
 (5, "Ravi", "HR", 72000, "14-06-2021")
]

"""create columns: emp_id, name, dept, salary, hire_date
#Question 1: What is the average salary by department?
Question 2: Find the highest salary in each department.
Question 3: Calculate the salary range (min and max) for employees in the IT department.
Question 4: List the names and salaries of employees who earn more than the average salary of
their department
Question 5: Find the employee with the 3rd highest salary"""

schema = StructType([
    (StructField("emp_id", IntegerType(), False)),
    (StructField("name", StringType(), False)),
    (StructField("dept", StringType(), False)),
    (StructField("salary", IntegerType(), False)),
    (StructField("hire_date", StringType(), False))
])

df = spark.createDataFrame(data, schema)
#df.show()

#1,2
df_avg = df.groupBy(col("dept")).agg(avg(col("salary")), max(col("salary")))
df_avg.show()

#3
df_IT = df.groupBy(col("dept")).agg(min("salary"), max("salary")).filter(col("dept")=="IT")
df_IT.show()

#4
df_win = Window.partitionBy(col("dept")).orderBy(col("salary"))
df_emp = df.withColumn("avg_sal", avg(col("salary")).over(df_win)).select(col("name"), col("salary")).where(col("salary") > col("avg_sal"))
df_emp.show()

#5
df_win = Window.orderBy(col("emp_id").desc())

df_3rd = df.withColumn("rnk", rank().over(df_win)).filter("rnk==3").drop("rnk")
df_3rd.show()



-----------pending(sql)--------

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Products6 (
# MAGIC
# MAGIC     ProductID INT,
# MAGIC
# MAGIC     Product VARCHAR(255),
# MAGIC
# MAGIC     Category VARCHAR(100)
# MAGIC
# MAGIC );
# MAGIC
# MAGIC
# MAGIC
# MAGIC INSERT INTO Products6 (ProductID, Product, Category)
# MAGIC
# MAGIC VALUES
# MAGIC
# MAGIC     (1, 'Laptop', 'Electronics'),
# MAGIC
# MAGIC     (2, 'Smartphone', 'Electronics'),
# MAGIC
# MAGIC     (3, 'Tablet', 'Electronics'),
# MAGIC
# MAGIC     (4, 'Headphones', 'Accessories'),
# MAGIC
# MAGIC     (5, 'Smartwatch', 'Accessories'),
# MAGIC
# MAGIC     (6, 'Keyboard', 'Accessories'),
# MAGIC
# MAGIC     (7, 'Mouse', 'Accessories'),
# MAGIC
# MAGIC     (8, 'Monitor', 'Accessories'),
# MAGIC
# MAGIC     (9, 'Printer', 'Electronics');

# COMMAND ----------

df = spark.sql("select * from products6")

df_win1 = Window.partitionBy(col("category")).orderBy(col("productid").desc())
df_win2 = Window.partitionBy(col("category")).orderBy(col("productid"))

df_products1 = df.withColumn("rn1", row_number().over(df_win1))
df_products2 = df.withColumn("rn2", row_number().over(df_win2))
df_joined = df_products1.alias("df1").join(df_products2.alias("df2"), (col("df1.rn1")==col("df2.rn2")) & (col("df1.Category")==col("df2.Category")), "inner")
df_final = df_joined.orderBy(col("df1.productid")).select(col("df1.productid"), col("df2.product"), col("df2.category"))
df_final.show()


----------pending(sql)----------

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employees6  (employee_id int,employee_name varchar(15), email_id varchar(15) );
# MAGIC INSERT INTO employees6 (employee_id,employee_name, email_id) VALUES ('101','Liam Alton', 'li.al@abc.com');
# MAGIC INSERT INTO employees6 (employee_id,employee_name, email_id) VALUES ('102','Josh Day', 'jo.da@abc.com');
# MAGIC INSERT INTO employees6 (employee_id,employee_name, email_id) VALUES ('103','Sean Mann', 'se.ma@abc.com'); 
# MAGIC INSERT INTO employees6 (employee_id,employee_name, email_id) VALUES ('104','Evan Blake', 'ev.bl@abc.com');
# MAGIC INSERT INTO employees6 (employee_id,employee_name, email_id) VALUES ('105','Toby Scott', 'jo.da@abc.com');
# MAGIC INSERT INTO employees6 (employee_id,employee_name, email_id) VALUES ('106','Anjali Chouhan', 'JO.DA@ABC.COM');
# MAGIC INSERT INTO employees6 (employee_id,employee_name, email_id) VALUES ('107','Ankit Bansal', 'AN.BA@ABC.COM');

# COMMAND ----------

df = spark.sql("select * from employees6")

df_win = Window.partitionBy(lower(col("email_id")))
df_emps = df.withColumn("rnk", dense_rank().over(df_win))
df_emps.show()


# COMMAND ----------

"""Write a Pyspark query to report the movies with an odd-numbered ID and a description that is not "boring".Return the result table in descending order by rating."""

data=[(1, 'War','great 3D',8.9)
,(2, 'Science','fiction',8.5) 
,(3, 'irish','boring',6.2)
,(4, 'Ice song','Fantacy',8.6)  
,(5, 'House card','Interesting',9.1)]    
schema="ID int,movie string,description string,rating double"

df = spark.createDataFrame(data, schema)
df.where((col("id")%2!=0) & (col("description")!="boring")).orderBy(col("rating").desc()).show()

# COMMAND ----------

# find count of chr greater than 1
input = "bbbbcccaad"

e = []
for i in input:
    count = input.count(i)
    if count not in e and count>=1:
        e.append(count)
print(e)

# COMMAND ----------

input = "susheelgajbinkar@gmail.com"
output = "s***********r@gmail.com"

# COMMAND ----------

data = [(12, "A", "D", "1-06"),
(13, "B", "C", "1-06"),
(14, "C", "D", "1-06"),
(13, "B", "C", "2-06"),
(14, "C", "D", "2-06"),
(12, "A", "C", "2-06"),
(14, "C", "D", "3-06")]

schema = "empid int, empname string, transaction string, timestamp string"

df = spark.createDataFrame(data, schema)

df_grp = df.groupBy(col("empid"), col("empname"), col("transaction")).agg(max(col("timestamp")))
df_grp.show()

-----------pending(sql)---------------