# Databricks notebook source
#To read the file data

df = spark.read.csv("/FileStore/tables/Order.csv", header=True, inferSchema=True, sep =',')
or
df = spark.read.format('csv').load("/FileStore/tables/Order.csv", header=True, inferSchema=True, sep =',')
or
df = spark.read.load("/FileStore/tables/Order.csv", format='csv', header=True)

# COMMAND ----------

#for printing schema

df.printSchema() - shows schema in programmatic manner
df.schema - shows schema in declaritive manner

# COMMAND ----------

#for printing dataframe

df.show() or display(df)    #generally we use display(df) function

#advantages of show
df.show(n=20, truncate=True, vertical=False)
we can leverage the truncate parameter, if it is set to False then the entire string of the columns will come in the result table, if set to true, by default the strings will be truncated to 20 and we can manually set the values as well (e.g truncate = 3).

If Vertical parameter is set to true, then the resultant output will be a vertical view.

# COMMAND ----------

#Define schema programatically

from pyspark.sql.types import *
orderSchema = StructType([StructField("Region", StringType() ,True)
,StructField("Country", StringType() ,True)
,StructField("ItemType", StringType() ,True)
,StructField("SalesChannel", StringType() ,True)
])

df = spark.read.load("/FileStore/tables/Order.csv",format="csv", header=True, schema=orderSchema)
df.printSchema()

# COMMAND ----------

#define schema declaritively

orderSchema = 'Region String ,Country String ,ItemType String ,SalesChannel String'

df = spark.read.load("/FileStore/tables/Order.csv",format="csv", header=True, schema=orderSchema)
df.printSchema()

# COMMAND ----------

#Show, take, collect: all are actions in Spark. Depends on our requirement and need we can opt any of these.

df.show() : It will show only the content of the dataframe.
df.collect() : It will show the content and metadata of the dataframe.
df.take(n) : shows content and structure/metadata for a limited number of rows for a very large dataset. Ex., df.take(5)

# COMMAND ----------

#To get list of columns

df.columns

# COMMAND ----------

#To get the first row

df.first()

# COMMAND ----------

#Creating a dataframe

from pyspark.sql import Row

row1 = Row("Ram", None, 1, True)
myManualSchema = 'Name string, address string not null, id integer, exists string'
manDf = spark.createDataFrame([row1], myManualSchema)
manDf.show()

# COMMAND ----------

#Returns a column based on the given column name. Both col and column are same functions, you can use any one

from pyspark.sql.functions import col, column
col("someColumnName")
column("someColumnName")

# COMMAND ----------

#Select expressions

from pyspark.sql.functions import expr
df.select("Region", "Country").show(2)

#select uisng variety of ways

df.select(expr("Country"),col('Region'),column('ItemType'),df.OrderID).show(2)

#select using the alias

df.select(expr("Country as NewCountry"), col("Region").alias('New Region'), column("ItemType"), df.OrderID).show(2)

#Use selectExpr

df.selectExpr("Country as NewCountry", "Region").show(2)

# COMMAND ----------

#Using literals

from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("NumberOne")).show(2)
df.select(expr("*"), lit(True).alias("MyResult")).show(2)
df.select(expr("*"), lit('Constant').alias("AddedColumn")).show(2)

# COMMAND ----------

#Adding new column to df

from pyspark.sql.functions import lit
newdf = df.withColumn("NewColumn", lit(1)).show(2)
newdf = df.withColumn("withinCountry", expr("Country == 'India'"))
newdf.columns

# COMMAND ----------

#Renaming columns

newdf = df.withColumnRenamed("Country", "NewCountry")
newdf.columns

#Use escape character i.e.,(`) when column name contains spaces
newdf =df.withColumnRenamed("Country","New Column Name")
newdf.select("`New Column Name`").show()

# COMMAND ----------

#Removing columns

df.drop("Country").columns
df.drop("Country", "Region").columns

# COMMAND ----------

#Change column type

df.withColumn("UnitsSoldNew", col("UnitsSold").cast("double")).show(2)

# COMMAND ----------

#Filtering rows

df.filter(col("UnitsSold") < 1000).show(2)
df.where("UnitsSold < 1000").show(2)
df.filter(df.UnitsSold < 1000).show(2)

Key Differences:
.filter() is more Pythonic and preferred for programmatic expressions.
.where() is more SQL-like, making it familiar for SQL users.

# COMMAND ----------

#Unique rows

df.select("Country", "Region").distinct().count()
df.select("Country").distinct().count()

# COMMAND ----------

#Union

df1 = df.filter("Country = 'Libya'")
df2 = df.filter("Country = 'Canada'")
df1.union(df2).show()

# COMMAND ----------

#Sort or orderBy - both serves same purpose

df.sort("Country").show(5)
df.orderBy("Country", "UnitsSold").show(5)
df.orderBy(col("ItemType"), col("UnitPrice")).show(5)

#asc and desc function
from pyspark.sql.functions import desc, asc
df.orderBy(expr("Country desc")).show(2)
df.orderBy(col("Country").desc(), col("UnitPrice").asc()).show(2)
df.sort("Country".desc()).show(2)


# COMMAND ----------

#Limit

df.limit(5).show()
df.orderBy(expr("Country desc")).limit(6).show()

# COMMAND ----------

#To get no. of partitions
df.rdd.getNumPartitions()

#repartition
df.repartition(5)
df.repartition("Country")
df.repartition(7, col("Country"))

#coalesce
df.coalesce(5)
#same examples


# COMMAND ----------

#Describe

df.describe()

# COMMAND ----------

#Translate the first letter of each word to upper case in the sentence
from pyspark.sql.functions import initcap, lower, upper
df.select(initcap(col("Country"))).show()

#To lower and upper
df.select(lower(col("Country"))).show()
df.select(upper(col("Country"))).show()

#To concat 
display(df.select(concat(col("Region"), col("Country"))))

#To concat with separater
display(df.select(concat_ws('|',col("Region"), col("Country"))))

#instr - Locate the position of the first occurrence of substr column in the given string
display(df.select(instr(col("Region"),"Mi")))

#length
df.length(col("Region"))

#trimming and padding
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
ltrim(lit(" HELLO ")).alias("ltrim"),
rtrim(lit(" HELLO ")).alias("rtrim"),
trim(lit(" HELLO ")).alias("trim"),
lpad(lit("HELLO"), 3, " ").alias("lp"),
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

#regexp_replace - Replace all substrings of the specified string value that match regexp with rep
from pyspark.sql.functions import regexp_replace
regex_string = "Hello|WHITE|RED|GREEN|BLUE"
df.select(regexp_replace(col("Country"), regex_string, "COLOR").alias("color_clean"),col("Description")).show(2)

# COMMAND ----------

#Date Handling in Spark

from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
.withColumn("today", current_date())\
.withColumn("now", current_timestamp())
display(dateDF)

#Add Subtract dates
from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

#Days and Month difference between dates
from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
.select(datediff(col("week_ago"), col("today"))).show(1)

dateDF.select(
to_date(lit("2016-01-01")).alias("start"),
to_date(lit("2017-05-22")).alias("end"))\
.select(months_between(col("start"), col("end"))).show(1)

#incorrect date format
dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

#Give date format
from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
to_date(lit("2017-12-11"), dateFormat).alias("date"),
to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.show()

#Handle timestamp to date  format casting
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, hour, minute, second
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

#get year using the year function with date and timepstamp
cleanDateDF.select(year(to_timestamp(col("date"), dateFormat))).show()

#get month using the month function with date and timepstamp
cleanDateDF.select(month(to_timestamp(col("date"), dateFormat))).show()

#get dayofmonth using the dayofmonth function with date and timepstamp
cleanDateDF.select(dayofmonth(to_timestamp(col("date"), dateFormat))).show()


#get hour using the hour function with date and timepstamp
cleanDateDF.select(hour(to_timestamp(col("date"), dateFormat))).show()


#get minute using the minute function with date and timepstamp
cleanDateDF.select(minute(to_timestamp(col("date"), dateFormat))).show()


#get second using the second function with date and timepstamp
cleanDateDF.select(second(to_timestamp(col("date"), dateFormat))).show()


# COMMAND ----------

#ifnull - Returns null if col1 equals to col2, or col1 otherwise
#nullif - Returns null if col1 equals to col2, or col1 otherwise
#nvl - Returns col2 if col1 is null, or col1 otherwise
#nvl2 - Returns col2 if col1 is not null, or col3 otherwise

%sql
SELECT
ifnull(null, 'return_value'),
nullif('value', 'value'),
nvl(null, 'return_value'),
nvl2('not_null', 'return_value', "else_value")
FROM dfTable LIMIT 1

# COMMAND ----------

#dropna - Returns a new DataFrame omitting rows with null values
df.na.drop()

#If ‘any’, drop a row if it contains any nulls. If ‘all’, drop a row only if all its values are null
df.na.drop("any")
df.na.drop("all")

#We can also apply this to certain sets of columns by passing in an array of columns:
df.na.drop("all", subset=["Country", "Region"])

# COMMAND ----------

#fillna() and fill() - both are same. Used to replace NULL/None values on all or selected multiple columns with either zero(0), empty string, space, or any constant literal values

df.na.fill(value=0).show()

#only on popluation column
df.na.fill(value=0, subset=["population"]).show()


# COMMAND ----------

#split - Splits str around matches of the given pattern

#split() with withColumn
df1 = df.withColumn('year', split(df['dob'], '-').getItem(0)) \
       .withColumn('month', split(df['dob'], '-').getItem(1)) \
       .withColumn('day', split(df['dob'], '-').getItem(2))
df1.show(truncate=False)

# split() with select()
split_col = pyspark.sql.functions.split(df['dob'], '-')
df3 = df.select("firstname","middlename","lastname","dob", split_col.getItem(0).alias('year'),split_col.getItem(1).alias('month'),split_col.getItem(2).alias('day'))   
df3.show(truncate=False)

# COMMAND ----------

#size - returns the length of the array or map stored in the column

#Get the size of a column to create another column
from pyspark.sql.functions import size
df.withColumn("lang_len",size(col("languages")))
  .withColumn("prop_len",size(col("properties")))
  .show(false)

# COMMAND ----------

#where and not null

df2 = df.select("Region", "Country", df["UnitsSold"].isNotNull()).where(df.Country == "Libya")

# COMMAND ----------

#explode - Returns a new row for each element in the given array or map. Uses the default column name col for elements in the array and key and value for elements in the map unless specified otherwise

from pyspark.sql.functions import split, explode
df.withColumn("splitted", split(col("Region"), " "))\
.withColumn("exploded", explode(col("splitted")))\
.select("Region", "Country", "exploded").show(2)

# COMMAND ----------

#create_map - columns can be grouped as key-value pairs

#Convert columns to Map
from pyspark.sql.functions import col,lit,create_map
df = df.withColumn("propertiesMap",create_map(
        lit("salary"),col("salary"),
        lit("location"),col("location")
        )).drop("salary","location")
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

#Aggregate functions

from pyspark.sql.functions import count, countDistinct, first, last, min, max, sum, sumDistinct, avg, var_pop, stddev_pop, var_samp, stddev_samp
df.select(count("Region")).show() 
df.select(countDistinct("Region")).show()
df.select(first("Region"), last("Region")).show()
df.select(min("UnitsSold"), max("UnitsSold")).show()
df.select(sum("UnitsSold")).show()
df.select(sumDistinct("UnitsSold")).show() 
df.select(avg("UnitsSold")).show() 
df.select(var_pop("Quantity"), var_samp("Quantity"),   #Variance and Standard Deviation for population and sample
stddev_pop("Quantity"), stddev_samp("Quantity")).show()

# COMMAND ----------

#groupBy - Groups the DataFrame using the specified columns, so we can run aggregation on them

from pyspark.sql.functions import count
df.groupBy("Region").agg(
count("UnitsSold").alias("quan"),
expr("count(UnitsSold)")).show()

# COMMAND ----------

/* Inner Join: Returns only the rows with matching keys in both DataFrames.
Left Join: Returns all rows from the left DataFrame and matching rows from the right DataFrame.
Right Join: Returns all rows from the right DataFrame and matching rows from the left DataFrame.
Full Outer Join: Returns all rows from both DataFrames, including matching and non-matching rows.
Left Semi Join: Returns all rows from the left DataFrame where there is a match in the right DataFrame.
Left Anti Join: Returns all rows from the left DataFrame where there is no match in the right DataFrame.*/

# Inner join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     .show(truncate=False)

# Left outer join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left")
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftouter")
    .show(truncate=False)

# Right outer join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right") \
   .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter") \
   .show(truncate=False)

# Full outer join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter") \
    .show(truncate=False)

# Left semi join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi") \
   .show(truncate=False)

# Left anti join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti") \
   .show(truncate=False)

# Self join
empDF.alias("emp1").join(empDF.alias("emp2"), \
    col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner") \
    .select(col("emp1.emp_id"),col("emp1.name"), \
      col("emp2.emp_id").alias("superior_emp_id"), \
      col("emp2.name").alias("superior_emp_name")) \
   .show(truncate=False)

# COMMAND ----------

#Write to dataframe

/* append: Appends the output files to the list of files that already exist at that location
overwrite: Will completely overwrite any data that already exists there
errorIfExists: Throws an error and fails the write if data or files already exist at the specified location
ignore: If data or files exist at the location, do nothing with the current DataFrame */

//Using string
personDF.write.mode("error").json("/path/to/write/person")

//Using option()
personDF.write.option("mode","error").json("/path/to/write/person")

//Using overwrite
personDF.write.mode("overwrite").json("/path/to/write/person")

#The overwrite mode is used to overwrite the existing file, Alternatively, you can use SaveMode.Overwrite. Using this write mode Spark deletes the existing file or drops the existing table before writing. When you are working with JDBC, you have to be careful using this option as you would lose indexes if exists on the table. To overcome this you can use truncate write option; this just truncates the table by keeping the indexes

//Using overwrite with truncate
personDF.write.mode("overwrite")
    .format("jdbc")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .option("url", "jdbc:mysql://localhost:3306/emp")
    .option("dbtable","employee")
    .option("truncate","true")
    .option("user", "root")
    .option("password", "root")
    .load()

//Using append
personDF.write.mode("append").json("/path/to/write/person")

# COMMAND ----------

#Explain

df.explain()

explain(mode=”simple”) – will display the physical plan
explain(mode=”extended”) – will display physical and logical plans (like “extended” option)
explain(mode=”codegen”) – will display the java code planned to be executed
explain(mode=”cost”) – will display the optimized logical plan and related statistics (if they exist)
explain(mode=”formatted”) – will display a split output composed of a nice physical plan outline and a section with each node details

# COMMAND ----------

#to enable AQE

spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

#Filter Functions

df2= df.filter(df.Country != "Libya")
df2.show()

#Multiple Conditions
df2= df.filter((df.Country  == "Libya") |  (df.Country  == "Japan"))
df2.show()

#Filter values
li=["Libya","Japan"]
df.filter(df.Country.isin(li)).show()

#Like Filter
df.filter(df.Country.like("%J%")).show()

#Regular Expression Filter
df.filter(df.Country.rlike("(?i)^*L$")).show()

#Assuming that one of the column in dataframe is Array, then You can run filter using the below code
from pyspark.sql.functions import array_contains
df.filter(array_contains(df.phoneNumbers,"123")).show()

# COMMAND ----------

#String Functions

from pyspark.sql import functions as F

df2 =df.select("Region","Country",F.when(df.UnitsSold > 2000, 1).otherwise(0))
df2.show()

#check isin
df2 = df[df.Country.isin("Libiya" ,"Japan")]
df2.show()

#Like
df2 = df.select("Region","Country", df.Country.like("L" ))
df2.show()

#StartsWith
df2 = df.select("Region","Country", df.Country.startswith("L"))
df2.show()
 
df2 = df.select("Region","Country", df.Country.endswith("L"))
df2.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls