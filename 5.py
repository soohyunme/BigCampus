# 전체 행에 대한 정의 필요
csv_file_name = "/user/ubuntu/data/ednet_s6_v1.csv"
skt = "/user/ubuntu/kim/skt4.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, column
spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

kt = spark.read.csv(csv_file_name,header=True,inferSchema=True)
kt = kt.drop(kt['count'])

# 기존 데이터프레임에 spent_time 조인
joined = kt.join(df_time,on=['index'],how='left_outer')

#play_video, 비율 변수 생성
video_df = \
kt.filter(kt.action_type=='play_video')\
.select('user_id','item_id').distinct()\
.groupBy('user_id').count()\
.withColumnRenamed('count','video_play_count')

#play_audio, 비율 변수 생성
audio_df = \
kt.filter(kt.action_type=='play_audio')\
.select('user_id','item_id').distinct()\
.groupBy('user_id').count()\
.withColumnRenamed('count','audio_play_count')


# 전체 기준 의논 후 수정 필요
# 유저별 전체 행동 개수(행 개수)
user_action = \
kt.groupBy('user_id').count()\
.withColumnRenamed('count','total_row_count_per_user')

# 하나의 DF로 조인
total = \
user_action.join(video_df,on=['user_id'],how='left_outer')\
.join(audio_df,on=['user_id'],how='left_outer')

# video_play_count Null 값 제거
total = \
total.withColumn("video_play_count", \
F.when(F.col("video_play_count").isNull(), 0).otherwise(F.col("video_play_count")))

# audio_play_count Null 값 제거
total = \
total.withColumn("audio_play_count", \
F.when(F.col("audio_play_count").isNull(), 0).otherwise(F.col("audio_play_count")))\
.select('user_id','video_play_count','audio_play_count','total_row_count_per_user')

# Null 값 제거 확인
for i in range(0,3):
    print('#',total[i+1],'에 있는 Null 값 개수 : ',total.filter(total[i+1].isNull()).count(),'\n')

# video 비율 구하기
video_play_ratio =\
total.withColumn("video_play_ratio", \
F.col('video_play_count')/F.col('total_row_count_per_user'))\
.select('user_id','video_play_ratio')

# audio 비율 구하기
audio_play_ratio =\
total.withColumn("audio_play_ratio", \
F.col('audio_play_count')/F.col('total_row_count_per_user'))\
.select('user_id','audio_play_ratio')


# 저장
video_play_ratio.write.csv('/user/ubuntu/kim/video_ratio.csv',header=True)
audio_play_ratio.write.csv('/user/ubuntu/kim/audio_ratio.csv',header=True)
