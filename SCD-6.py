from pyspark.sql import SparkSession,functions as F
from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import current_timestamp, to_date, lit, when, coalesce, greatest, least,udf, lit, when, date_sub
import json
from pyspark.sql import Row
from pyspark.sql import Window
from datetime import datetime
from pyspark.sql import SQLContext
from os.path import abspath
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType, BooleanType, DateType
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# create a SparkSession
spark = SparkSession \
        .builder \
        .appName('SCD Type 2') \
        .master('local[*]') \
        .config("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java.jar") \
        .config("spark.jars", "/usr/share/java/mysql-connector-java.jar") \
        .enableHiveSupport() \
        .getOrCreate()
        
spark.conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")


df_src = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/SCD6").option("driver","com.mysql.jdbc.Driver").option("dbtable","customer_src").option("user","spark").option("password",'spark').load()



df_trgt=spark.sql("select * from customers_trgts")



#history_data=df_src.join(df_trgt,df_src.src_id==df_trgt.id,"outer").filter("end_date is not null")

#history_data.show()


joined_data=df_src.join(df_trgt,df_src.src_id==df_trgt.id,"outer")

end_date = datetime.strptime('9999-12-31', '%Y-%m-%d').date()
start_date=current_timestamp()
 
joined_data = joined_data.withColumn('action',
 when(joined_data.key.isNull(), 'I')
.when((joined_data.src_address != joined_data.current_address) & (joined_data.end_date == '9999-12-31'), 'EI')
.when((joined_data.src_address != joined_data.current_address) & (joined_data.end_date != '9999-12-31'), 'U')
.otherwise('no_action')
)

joined_data.show()

df_insert=joined_data.filter(joined_data.action =="I").select(joined_data.src_id.alias("id"),joined_data.src_name.alias("name"),joined_data.src_address.alias("current_address"),joined_data.src_address.alias("prev_address")).withColumn("start_date",lit(start_date)).withColumn("end_date",lit(end_date))


df_ext_insert=joined_data.filter(joined_data.action == "EI").select(joined_data.src_id.alias("id"),joined_data.src_name.alias("name"),joined_data.src_address.alias("current_address"),joined_data.src_address.alias("prev_address")).withColumn("start_date",lit(start_date)).withColumn("end_date",lit(end_date))

df_ext_update=joined_data.filter(joined_data.action == "EI").select(joined_data.id,joined_data.name,joined_data.src_address.alias("current_address"),joined_data.prev_address,joined_data.start_date,joined_data.start_date.alias("end_date"))

df_update=joined_data.filter(joined_data.action == "U").select(joined_data.id,joined_data.name,joined_data.src_address.alias("start_date"),joined_data.prev_address,joined_data.start_date,joined_data.end_date)

df_no_action=joined_data.filter(joined_data.action == "no_action").select(joined_data.id,joined_data.name,joined_data.current_address,joined_data.prev_address,joined_data.start_date,joined_data.end_date)


df_final_result=df_insert.unionAll(df_ext_insert).unionAll(df_ext_update).unionAll(df_update).unionAll(df_no_action)
df_final_result.show()


window = Window.orderBy(F.col('id'))

df_final_result = df_final_result.withColumn('key', F.row_number().over(window))

df_final_result.show()



df_final_result.write.mode("overwrite").option("path", "/spark_data/temp/").saveAsTable("temp_customers_trgt")

df_final_store =spark.sql("select * from temp_customers_trgt")

df_final_store.write.mode('overwrite').option("path", "/spark_data/data/").saveAsTable("customers_trgts") 

spark.stop()


#change_type= when(joined_data["key"].isNull(),"I").when(joined_data["address"] != joined_data["prev_address"] &,"IU").otherwise("no_change")
