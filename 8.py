csv_file_name = "/user/ubuntu/data/ednet_s6_v3.csv"
v2 = "/user/ubuntu/data/ednet_s6_v2.csv"


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, column

spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

kt = spark.read.csv(csv_file_name,header=True,inferSchema=True)
v2 = spark.read.csv(v2,header=True,inferSchema=True)
v2 = v2.orderBy(v2['index'])
kt = kt.orderBy(kt['index'])
q_df = spark.read.csv("/user/ubuntu/kim/answer_df.csv",header=True,inferSchema=True)

joined = kt.join(q_df, on=['item_id'],how='left_outer')
correct_F = joined.withColumn("correct_flag", F.when(col("user_answer") == col("correct_answer"), 1).otherwise(0))
correct_F = correct_F.drop(correct_F['correct_answer'])
#correct_F.show()

kt8 = correct_F.select('index','user_id','action_type','item_id','correct_flag')

# 문제에 대해 정답을 체크한 적이 있는지 판별하는 for문
data_list = []
past_answer = ''

for row in kt8.collect() :
    if row.action_type == 'respond' and row.correct_flag == 1 :
        if past_answer != row.item_id:
            past_answer = row.item_id
            data_list.append([row['index'],1])

# 데이터프레임으로 변환

# 정답을 체크한 행들의 데이터 프레임
check_df = spark.createDataFrame(data_list, schema=['index','check_answer_flag'])
#aa.show()

# 유저 당 풀이한 전체 문제의 수 respond 중복 제거
unique_df = kt8.select('index','user_id','item_id').filter(kt8.action_type=='respond')

######################## 동일한 문제를 2번 풀었을 수도 있으니까 
ss = v2.join(check_df,on=['index'],how='left_outer')

# 전체 데이터에 체크한 행들 표시한 데이터프레임

user_data = [] # 인덱스 뽑기

for row in ss.collect() :
    if row.action_type == 'enter' :
        respond_flag = False
    elif (row.action_type != 'submit') and (respond_flag == True) :
        continue # 플래그 온 돼있으면 submit/quit 만날 때까지 모든 행 뛰어넘음
    elif (row.action_type == 'respond' and row.check_answer_flag == 1) : # enter와 submit/quit 사이에 respond가 나오면
        user_data = [row['index']] # 문제 진입하고 첫 정답 respond 행의 index를 임시 변수에 저장
        respond_flag = True # 플래그 온
    elif (row.action_type == 'submit' or row.action_type=='quit') :
        respond_flag = False

aaa = spark.createDataFrame(user_data, schema=['index'])
#############################

#제대로 걸러졌나 행 수 확인
unique_df.count()
aa.count()


# kt8을 기준으로 left_outer join
# 정답을 맞춘 행은 check_answer_flag에 1이 들어갈 것이고 아닌 행은 Null이 들어감
# 처리를 통해 Null을 1로 변경
new = unique_df.join(aa,on=['index'],how='left_outer')

# 조인이 잘 됐는지 행 수 비교 두 값이 다를 경우 이상하게 조인된 경우이므로
new.count()
unique_df.count()
new.show()

# Null 값 제거
new_df = new.withColumn("check_answer_flag", F.when(F.col("check_answer_flag").isNull(), 0).otherwise(F.col("check_answer_flag"))).select('user_id','check_answer_flag')


# 유저별 정답 체크 비율 게산
mean_df = \
new_df.groupBy(['user_id']).mean()\
.select('user_id','avg(check_answer_flag)')\
.withColumnRenamed('avg(check_answer_flag)','check_answer_rate')

mean_df.show()

# 저장
mean_df.write.csv('/user/ubuntu/data/check_answer_rate.csv',header=True)

# 저장 확인
qq = spark.read.csv('/user/ubuntu/data/check_answer_rate.csv',header=True)

# 확인
qq.show()