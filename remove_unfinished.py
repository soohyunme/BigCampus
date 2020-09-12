# enter와 submit/quit 세트에서 submit/quit이 없는 경우가 있음
csv_file_name = "/user/ubuntu/data/ednet_s6_v1.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, column
spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

kt = spark.read.csv(csv_file_name,header=True,inferSchema=True)

user_data = []
row_tmp = []
#spent_time_list = []
# enter와 submit/quit 사이의 행인지 플래그
enter_flag=True 

for row in kt.collect() :
    if row.action_type == 'enter' : # action_type이 enter면
        if enter_flag == True: # enter_flag가 True, 즉 submit/quit이 나오지 않았는데 enter가 나오면
            row_tmp = [] # row_tmp를 비워줌
        #enter_time = row.timestamp # 아마 여기에 시간tmp 넣으면 될듯
        enter_flag = True # enter가 나왔음 표시
        row_tmp.append(row) # enter 행을 임시변수에 추가
    elif (row.action_type == 'submit' or row.action_type == 'quit'):
        enter_flag = False # enter - submit/quit 세트가 완성되었음
        row_tmp.append(row) # submit/quit 행을 임시변수에 추가
        user_data.extend(row_tmp)# 임시 변수의 값들을 user_data에 붙임
        row_tmp = [] # row_tmp 초기화
    else : # action_type이 enter/submit/quit가 아닌 행
        row_tmp.append(row) # 기타 행을 임시변수에 추가

# 데이터프레임으로 변환
# 정답을 체크한 행들의 데이터 프레임
aa = spark.createDataFrame(user_data)
aa.show()

kt.count()
aa.count()

# 저장
aa.write.csv('/user/ubuntu/data/ednet_s6_v3.csv',header=True)

"""        
kt = spark.read.csv('/user/ubuntu/data/ednet_s6_v3.csv',header=True,inferSchema=True)
kt.orderBy(kt['index']).write.csv('/user/ubuntu/kim/ednet_s6_v3.csv',header=True)

aa = spark.read.csv('/user/ubuntu/kim/ednet_s6_v3.csv',header=True,inferSchema=True)
aa.show()

# 미완료 index 찾는 코드
flag = False
for row in aa.collect() :
    if row.action_type == 'enter' :
        tmp = row['index']
        if flag == True :
            print(tmp)
        flag = True
    elif row.action_type == 'submit':
        flag = False
    elif row.action_type == 'quit' : 
        flag = False


 """

#####################

v3 = "/user/ubuntu/data/ednet_s6_v3.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, column
spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.read.csv(v3,header=True,inferSchema=True)
df = df.orderBy(df['index'])
df.show()

user_data = [] #
row_tmp = [] #
spent_time_list = [] #
enter_flag = True #enter와 submit/quit 사이의 행인지 플래그

for row in df.collect() :
    if row.action_type == 'enter' :
        if enter_flag == True: #enter_flag가 True, 즉 submit/quit이 나오지 않았는데 enter가 나오면
            row_tmp = [] #row_tmp를 비워줌
            enter_time = row.timestamp #아마 여기에 시간tmp 넣으면 될듯
        enter_flag = True #enter가 나왔음 표시 ???
        row_tmp.append(row) #enter 행을 임시변수에 추가 ???
    elif (row.action_type == 'submit' or row.action_type == 'quit') :
        enter_flag = False #enter - submit/quit 세트가 완성되었음
        row_tmp.append(row) #submit/quit 행을 임시변수에 추가
        user_data.extend(row_tmp) #임시 변수의 값들을 user_data에 붙임
        row_tmp = [] # row_tmp 초기화
        spent_time_list.append([row.timestamp - enter_time])
    else : # action_type이 enter/submit/quit가 아닌 행
        row_tmp.append(row) # 기타 행을 임시변수에 추가
        
# 데이터프레임으로 변환
# 정답을 체크한 행들의 데이터 프레임
df_timespent = spark.createDataFrame(spent_time_list)
df_timespent.show()

df.count()
aa.count()
        
