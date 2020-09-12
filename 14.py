# erase_choice / undo_erase_choice 빈도수/비율 세서 변수 생성 
# 유저 당 erase, undo, erase+undo / 전체 행

csv_file_name = "/user/ubuntu/data/ednet_s6_v2.csv"
skt_name = "/user/ubuntu/kim/skt4.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, column

spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

kt = spark.read.csv(csv_file_name,header=True,inferSchema=True)
#skt = spark.read.csv(skt_name,header=True,inferSchema=True)

# 유저 당 삭제 가능한 보기의 전체 수 (풀이한 전체 문제 수 *4)
user_total = kt.groupBy('user_id').count().withColumnRenamed('count','total_count')

# erase_choice 횟수
erase_count = \
kt.filter(kt['action_type']=='erase_choice')\
.select('user_id','item_id')\
.groupBy('user_id').count()\
.withColumnRenamed('count','erase_count')

# undo_erase_choice 횟수
undo_count = \
kt.filter(kt['action_type']=='undo_erase_choice')\
.select('user_id','item_id')\
.groupBy('user_id').count()\
.withColumnRenamed('count','undo_count')

############################################
# 전체 문제에 대해 얼마나 많이 erase를 하였는가? => erase에 대한 비율 변수 생성
# user_total에 erase_count left join
join_total_erase = \
user_total.join(erase_count,on=['user_id'],how='left_outer')\
.select('user_id','erase_count','total_count')

# Null 값 제거
join_total_erase = \
join_total_erase.withColumn("erase_count", \
F.when(F.col("erase_count").isNull(), 0).otherwise(F.col("erase_count")))

# 비율 열 생성 erase_ratio에 저장
erase_ratio = \
join_total_erase.withColumn("erase_ratio", \
F.col("erase_count")/F.col("total_count"))\
.select('user_id','erase_ratio')

############################################
# 전체 문제에 대해 얼마나 많이 undo를 하였는가? => undo에 대한 비율 변수 생성
# user_total에 undo_count left join
join_total_undo = \
user_total.join(undo_count,on=['user_id'],how='left_outer')\
.select('user_id','undo_count','total_count')

# Null 값 제거
join_total_undo = \
join_total_undo.withColumn("undo_count", \
F.when(F.col("undo_count").isNull(), 0).otherwise(F.col("undo_count")))

# 비율 열 생성 undo_ratio에 저장
undo_ratio = \
join_total_undo.withColumn("undo_ratio", \
F.col("undo_count")/F.col("total_count"))\
.select('user_id','undo_ratio')


############################################
# 전체 문제에 대해 얼마나 많이 erase와 undo를 하였는가? => erase와 undo에 대한 비율 변수 생성
# user_total에 erase_count left join
join_total_both = \
user_total.join(erase_count,on=['user_id'],how='left_outer')\
.select('user_id','erase_count','total_count')

# user_total에 undo_count left join
join_total_both = \
join_total_both.join(undo_count,on=['user_id'],how='left_outer')\
.select('user_id','undo_count','erase_count','total_count')

# undo Null 값 제거
join_total_both = \
join_total_both.withColumn("undo_count", \
F.when(F.col("undo_count").isNull(), 0).otherwise(F.col("undo_count")))

# erase Null 값 제거
join_total_both = \
join_total_both.withColumn("erase_count", \
F.when(F.col("erase_count").isNull(), 0).otherwise(F.col("erase_count")))

# 합계 열 생성 erase_undo_sum
join_total_both_new = \
join_total_both.withColumn("erase_undo_sum", \
(F.col('undo_count')+F.col('erase_count')))\
.select('user_id','erase_undo_sum','total_count')

# 비율 열 생성 erase_undo_ratio 저장
both_ratio = \
join_total_both_new.withColumn("erase_undo_ratio", \
F.col('erase_undo_sum')/F.col('total_count'))\
.select('user_id','erase_undo_ratio')

# 결과 데이터프레임들을 저장

erase_ratio.write.csv('/user/ubuntu/data/erase_ratio.csv',header=True)
undo_ratio.write.csv('/user/ubuntu/data/undo_ratio.csv',header=True)
both_ratio.write.csv('/user/ubuntu/data/erase_undo_ratio.csv',header=True)
