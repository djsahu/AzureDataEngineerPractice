# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Write a solution to display the records with three or more rows with consecutive id's, and the number of people is greater than or equal to 100 for each.

# COMMAND ----------

sch=StructType([
 StructField("id",IntegerType(),True),
 StructField("visit_date ",StringType(),True),
 StructField("people",IntegerType(),True)
])

data = [
 (1, "2017-01-01", 10),
 (2, "2017-01-02", 200),
 (3, "2017-01-03", 300),
 (4, "2017-01-04", 99),
 (5, "2017-01-05", 101),
 (6, "2017-01-06", 123),
 (7, "2017-01-07", 189),
 (8, "2017-01-09", 163)
]

df=spark.createDataFrame(data,sch)
df.show()

# COMMAND ----------

def sequence_record(df):
 Wnd_spc=Window.orderBy(col('visit_date '))
 filter_df=df.filter(col('people')>=100)\
 .withColumn("rowNum",col('id')-row_number().over(Wnd_spc))\
 .withColumn('cnt',count('*').over(Window.partitionBy(col('rowNum'))))\
 .filter(col('cnt')>=3)\
 .select(['id','visit_date ','people'])
 
 return filter_df.orderBy(col('visit_date '))

result_df=sequence_record(df)
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC PySpark program to find employees whose salary is more than their departments average salary

# COMMAND ----------

data=[(1,'Mahesh','IT',22000),(2,'Payal','HR',13000),(3,'Satish','IT',25000),(4,'Amit','IT',24000),(5,'Swetha','HR',14000),(6,'Minal','IT',23000)]

schema='e_id INT,emp_name STRING,e_dept STRING,salary INT'

df=spark.createDataFrame(data=data,schema=schema)
df.show()

# COMMAND ----------

df.createOrReplaceTempView('employee_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC with avg_sal as (
# MAGIC select e_dept, AVG(salary) as avg_salary from employee_df
# MAGIC group by e_dept
# MAGIC )
# MAGIC select edf.* from employee_df as edf
# MAGIC JOIN avg_sal as avg
# MAGIC ON edf.e_dept=avg.e_dept
# MAGIC WHERE edf.salary>avg.avg_salary

# COMMAND ----------

avg_dep=df.groupby(col('e_dept')).agg(avg(col('salary')).alias("avg_salary")).select('e_dept','avg_salary')
sol=df.join(avg_dep, on='e_dept').filter(col('salary')>col('avg_salary')).select('e_id','emp_name','e_dept','salary')
sol.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Count length of each word in the dataframe

# COMMAND ----------

data = [("john",), ("alice",), ("bob",)]
dfCount = spark.createDataFrame(data, ["name"])

# COMMAND ----------

dfCount.withColumn('word_len',length(col('name'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Find the topper in each semester.

# COMMAND ----------

data = [
 (1,'A',1,'PHYSICS',100),
 (1,'A',2,'PHYSICS',150),
 (1,'A',3,'PHYSICS',200),
 (1,'A',4,'PHYSICS',250),
 (1,'A',1,'CHEMISTRY',50),
 (1,'A',2,'CHEMISTRY',250),
 (1,'A',3,'CHEMISTRY',200),
 (1,'A',4,'CHEMISTRY',350),
 (2,'B',1,'PHYSICS',150),
 (2,'B',2,'PHYSICS',250),
 (2,'B',3,'PHYSICS',100),
 (2,'B',4,'PHYSICS',200),
 (2,'B',1,'CHEMISTRY',150),
 (2,'B',2,'CHEMISTRY',150),
 (2,'B',3,'CHEMISTRY',250),
 (2,'B',4,'CHEMISTRY',300),
]
schema = ['id','name','semester','subject','marks']

# COMMAND ----------

exam_df= spark.createDataFrame(schema=schema,data=data)
exam_df.show()

# COMMAND ----------

sem_marks = exam_df.groupby(col('name'),col('semester')).agg(sum(col('marks')).alias('tot_marks'))
# sem_marks.show()
max_sem_marks=sem_marks.groupBy(col('semester')).agg(max(col('tot_marks')).alias('max_marks'))
sem_marks.join(max_sem_marks,on='semester').filter(col('tot_marks')==col('max_marks')).orderBy('semester').select('name','semester','tot_marks').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Display 'INDIA' at top and other countries in ascending order

# COMMAND ----------

data = [
 ('INDONESIA',),
 ('NEPAL',),
 ('CHINA',),
 ('PAKISTAN',),
 ('SRI LANKA',),
 ('INDIA',),
 ('SINGAPORE',),
 ('AFGHANISTAN',),
 ('UNITED KINGDOM',),
 ('RUSSIA',),
]
schema = ['name']

team_df = spark.createDataFrame(data=data,schema=schema)
team_df.show()

# COMMAND ----------

team_df.orderBy(when(col('name')=='INDIA',1).otherwise(col('name'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write a solution to display the records with three or more rows with consecutive id's, and the number of people is greater than or equal to 100 for each.

# COMMAND ----------

data=[(1,'Mahesh','IT',22000),(2,'Payal','HR',13000),(3,'Satish','IT',25000),(4,'Amit','IT',24000),(5,'Swetha','HR',14000),(6,'Minal','IT',23000)]

schema='e_id INT,emp_name STRING,e_dept STRING,salary INT'

emp_df=spark.createDataFrame(data=data,schema=schema)
emp_df.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Write a PySpark program to find records of customers who have purchased only one brand of biscuits.

# COMMAND ----------

values=[(1,'Parle','Parle-G'),(1,'Britannia','Marie Gold'),(1,'ITC','Marie Lite'),(1,'Anmol','Dream Lite'),\
    (2,'Unibic','Choco-chip Cookies'),\
(2,'Unibic','Butter Cookies'),(3,'Cremica','Jira Lite'),(4,'ITC','Marie Lite'),\
    (4,'Britannia','Marie Gold'),(5,'Unibic','Cashew Cookies'),\
(5,'Unibic','Butter Cookies'),(5,'Unibic','Fruit & nut Cookies')]

sc=StructType().add(field='customer_id',data_type=IntegerType(),nullable=False).add(field='brand_name',data_type=StringType(),nullable=False)\
    .add(field='prod_name',data_type=StringType(),nullable=False)

df=spark.createDataFrame(data=values,schema=sc)
df.show()


# COMMAND ----------

cust_count=df.groupBy(col('customer_id')).agg(count('*').alias('c'))
prod_count=df.groupBy(col('customer_id'),col('brand_name')).agg(count('*').alias('p_c'))
# prod_count.groupBy(col('customer_id')).agg(count('*').alias('cnt')).filter(col('cnt')==1).select('customer_id').show()
cust_count.join(prod_count,on='customer_id').filter(col('c')==col('p_c')).select('customer_id','brand_name').show()

# COMMAND ----------


