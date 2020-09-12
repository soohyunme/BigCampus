from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.read.csv('/user/ubuntu/data_big/EDNET.csv', header=True)
df.show()

from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

df_index = df.withColumn('index', row_number().over(Window.orderBy(monotonically_increasing_id())))
#df_index.tail(1)
#df.count()

# 저장
df_index = df_index.select('index','user_id','timestamp','action_type','item_id','cursor_time','source','user_answer','platform')
df_index.write.csv('/user/ubuntu/kim/aaa.csv',header=True)

# 단일 파일로 저장되기 때문에 불러와서 다시 분산 저장해줘야함

aa = spark.read.csv("/user/ubuntu/kim/aaa.csv", header=True, inferSchema=True)
aa.write.csv("/user/ubuntu/data_big/EDNET_index.csv", header=True)



aa = spark.read.csv("/user/ubuntu/data_big/EDNET_index.csv", header=True, inferSchema=True)
