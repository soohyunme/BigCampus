# user별 review 행 수 / enter/submit/qiut 제거한 행 수
csv_file_name = "/user/ubuntu/data/ednet_s6_v2.csv"
skt_name = "/user/ubuntu/kim/skt4.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, column

spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

kt = spark.read.csv(csv_file_name,header=True,inferSchema=True)
#skt = spark.read.csv(skt_name,header=True,inferSchema=True)
#origin = spark.read.csv("/user/ubuntu/data/kt_s6_v1.csv",header=True,inferSchema=True)

# user 별 전체 행 개수
user_total = kt.groupBy('user_id').count().withColumnRenamed('count','total_row_count')

# user 별 review 행 개수
user_review = kt.filter(kt.source =='review').groupBy('user_id').count().withColumnRenamed('count','review_count')

# 전체 행과 review join
joined = user_total.join(user_review,on=['user_id'],how='full_outer')

# joined Null 값 확인
print('# total row Null count :',\
joined.filter(joined['total_row_count'].isNull()).count(),\
'\n# review Null count :',\
joined.filter(joined['review_count'].isNull()).count())

# joined - review_count Null 값 제거
joined = \
joined.withColumn("review_count", \
F.when(F.col("review_count").isNull(), 0).otherwise(F.col("review_count")))

# joined Null 값 확인
print('# total row Null count :',\
joined.filter(joined['total_row_count'].isNull()).count(),\
'\n# review Null count :',\
joined.filter(joined['review_count'].isNull()).count())

# user 별 복습 비율 계산
# in_review 행 개수 / 전체 행 개수
review_ratio = \
joined.withColumn("review_ratio", \
F.col('review_count')/F.col('total_row_count'))\
.select('user_id','review_ratio')

# 저장
review_ratio.write.csv('/user/ubuntu/data/review_ratio.csv',header=True)

# 저장 확인
aa = spark.read.csv('/user/ubuntu/data/review_ratio.csv',header=True,inferSchema=True)
aa.show()