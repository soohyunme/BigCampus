# 재생 시간 이동 시 기록 남는 것 해결해야함

csv_file_name = "/user/ubuntu/data/ednet_s6_v1.csv"
#skt_name = "/user/ubuntu/kim/skt4.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, column

spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

kt = spark.read.csv(csv_file_name,header=True,inferSchema=True)
#skt = spark.read.csv(skt_name,header=True,inferSchema=True)
#origin = spark.read.csv("/user/ubuntu/data/kt_s6_v1.csv",header=True,inferSchema=True)

# user 별 전체 행 개수
user_total = kt.groupBy('user_id').count().withColumnRenamed('count','total_row_count')

########################################
# 일시정지
# 비디오 일시정지 횟수 구하기
pause_video_count = \
kt.filter(kt['action_type']=='pause_video')\
.select('user_id','item_id')\
.groupBy('user_id').count()\
.withColumnRenamed('count','pause_video_count')

# 오디오 일시정지 횟수 구하기
pause_audio_count = kt.filter(kt['action_type']=='pause_audio')\
.select('user_id','item_id')\
.groupBy('user_id').count()\
.withColumnRenamed('count','pause_audio_count')

######################################
# 비디오 일시정지 비율 구하기
join_pause_video = \
user_total.join(pause_video_count,on=['user_id'],how='left_outer')\
.select('user_id','pause_video_count','total_row_count')

# Null 값 제거
join_pause_video = \
join_pause_video.withColumn("pause_video_count", \
F.when(F.col("pause_video_count").isNull(), 0).otherwise(F.col("pause_video_count")))

# 비율 열 생성 pause_video_ratio 저장
pause_video_ratio = \
join_pause_video.withColumn("pause_video_ratio", \
F.col("pause_video_count")/F.col("total_row_count"))\
.select('user_id','pause_video_ratio')

######################################
# 오디오 일시정지 비율 구하기
join_pause_audio = \
user_total.join(pause_audio_count,on=['user_id'],how='left_outer')\
.select('user_id','pause_audio_count','total_row_count')

# Null 값 제거
join_pause_audio = \
join_pause_audio.withColumn("pause_audio_count", \
F.when(F.col("pause_audio_count").isNull(), 0).otherwise(F.col("pause_audio_count")))

# 비율 열 생성 pause_audio_ratio 저장
pause_audio_ratio = \
join_pause_audio.withColumn("pause_audio_ratio", \
F.col("pause_audio_count")/F.col("total_row_count"))\
.select('user_id','pause_audio_ratio')

# 결과 데이터프레임들을 저장

pause_video_ratio.write.csv('/user/ubuntu/data/pause_video_ratio.csv',header=True)
pause_audio_ratio.write.csv('/user/ubuntu/data/pause_audio_ratio.csv',header=True)
