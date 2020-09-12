csv_file_name = "/user/ubuntu/data/ednet_s6_v3.csv"
skt = "/user/ubuntu/kim/skt4.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, column
spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

kt = spark.read.csv(csv_file_name,header=True)
#skt = spark.read.csv(skt,header=True,inferSchema=True)
q_df = spark.read.csv("/user/ubuntu/kim/answer_df.csv",header=True)
kt = kt.orderBy(kt['index'])

joined = kt.join(q_df, on=['item_id'],how='left_outer')
correct_F = joined.withColumn("correct_flag", F.when(col("user_answer") == col("correct_answer"), 1).otherwise(0))
correct_F = correct_F.drop(correct_F['correct_answer'])
#correct_F = correct_F.drop(correct_F['count'])
#correct_F.show()
kt7 = correct_F.select('index','user_id','action_type','item_id','correct_flag')

user_data = []
flag = False

# 문제 제출 시의 값(최종으로 선택한 행의 인덱스)을 리스트에 저장
for row in kt7.collect() :
    if row.action_type == 'respond' :
        row_tmp = row
        flag = True
    elif (row.action_type == 'submit' or row.action_type == 'quit') and flag == True:
        user_data.append([row_tmp['index']])
        flag = False

aa = spark.createDataFrame(user_data, schema=['index'])

# 제대로 걸러졌나 확인
kt7.filter(kt7.action_type=='respond').count()
aa.count()

# 기존 데이터프레임에서 최종 제출에 대한 행만 선택 inner join 
new = kt7.join(aa,on=['index'],how='inner')
#new.show()

# user 별로 그룹하여 맞은 개수와 틀린 개수로 평균(정답률) 계산
mean_df = new.groupBy(['user_id']).mean()
#mean_df.show()
correct_pct = mean_df.withColumnRenamed('avg(correct_flag)','correct_pct')

# 저장
correct_pct.write.csv('/user/ubuntu/data/correct_pct.csv',header=True)

# 저장 확인
qq = spark.read.csv('/user/ubuntu/data/correct_pct.csv',header=True)

# 확인
kt.filter(kt.action_type=='respond').select('user_id').distinct().count()
qq.count()
