# 극단치 제거

# enter와 submit/quit 세트에서 submit/quit이 없는 경우가 있음
csv_file_name = "/user/ubuntu/data_big/EDNET.csv"
skt_name = "/user/ubuntu/kim/skt4.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, column
spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

kt = spark.read.csv(csv_file_name,header=True)
#skt = spark.read.csv(skt_name,header=True,inferSchema=True)

# respond에 대한 전체 행 수
kt.filter(kt.action_type=='respond').select(kt.user_id,kt.item_id).count()

# user 당 풀이한 문제의 수
user_respond_count =\
kt.filter(kt.action_type=='respond')\
.select(kt.user_id,kt.item_id).distinct()\
.groupBy('user_id').count()\
.withColumnRenamed('count','respond_per_user')

# 전체 유저 수
print('# 전체 user 수 :', user_respond_count.count())
# 296701

# 문제 풀이 개수가 20개 이상인 유저 수
i = 20
print('#',i,'문제 풀이 개수가 20개 이상인 유저 수',\
user_respond_count.filter(user_respond_count.respond_per_user>i).count())
filter_df = user_respond_count.filter(user_respond_count.respond_per_user>i)
# 20개 미만 유저 수 : 228553
# 20개 이상 유저 수 : 65917

# kt에 대해 inner 조인 - 둘 다 있는 user_id에 대해서만 남겨둠
joined = kt.join(filter_df,on=['user_id'],how='inner')

# 잘 제거되었는지 확인
aa = joined.filter(joined.action_type=='respond')\
.select(joined.user_id,joined.item_id).distinct()\
.groupBy('user_id').count()\
.withColumnRenamed('count','respond_per_user')
aa.filter(aa.respond_per_user<20).count()

# 전체 행 수 확인
kt.count()
# 삭제 후 전체 행 수 확인
joined.count()

# 저장
joined.write.csv('/user/ubuntu/data_big/EDNET.csv',header=True)

# 확인
bb = spark.read.csv('/user/ubuntu/data_big/EDNET.csv',header=True)
bb.show()
