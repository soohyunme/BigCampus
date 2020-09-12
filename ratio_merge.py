# 비율 변수를 kt의 모든 user_id에 붙이는 코드

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, column
from pyspark.sql.functions import isnan, when, count, col

spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

csv_kt_file_name = "/user/ubuntu/data/ednet_s6_v1.csv"
kt = spark.read.csv(csv_kt_file_name,header=True,inferSchema=True)

# kt에서 user_id만 선택
kt=kt.select('user_id').distinct()

# 아래 리스트에 결합할 파일들을 입력
file_name_list = \
['correct_pct.csv','archive_rate.csv','change_answer_rate.csv'\
,'adaptive_rate.csv','video_audio_ratio.csv','check_answer_rate.csv'\
,'erase_ratio.csv','undo_ratio.csv','erase_undo_ratio.csv','review_ratio.csv'\
,'pause_video_ratio.csv','pause_audio_ratio.csv','avg_spent_time.csv']
ratio_df = []
file_count = 1

# 자동 변수 선언
# ratio1, ratio2, --- 에 각각의 데이터프레임 저장
for i in range(0, len(file_name_list)):
    globals()['ratio{}'.format(i+1)] = spark.read.csv("/user/ubuntu/data/"+file_name_list[i],header=True,inferSchema=True)


# 기존 파일과 각각의 파일들의 user_id 수 비교
for i in range(0, len(file_name_list)):
    if i ==0:
        print('\t',kt.count(),'<=  기존 kt 파일 안에 있는 user의 수 ')
    print('\t',globals()['ratio{}'.format(i+1)].count(),'<= ',file_name_list[i],'파일 안에 있는 user의 수')


# 불러온 데이터프레임을 total 데이터프레임에 모두 조인
# 조인하는 과정에서 Null 값도 제거
for i in range(0, len(file_name_list)):
    if i == 0:
        total = kt # 첫 실행 시 total을 kt 데이터로 초기화
    # total에 ratio[i+1]번째 데이터프레임을 left outer join
    total = \
    total.join(globals()['ratio{}'.format(i+1)],on=['user_id'],how='left_outer')
    # 로그 출력
    print('# total 데이터프레임에',file_name_list[i],'조인')
    # Null 값 제거
    total = \
    total.withColumn(total.columns[i+1], \
    F.when(F.col(total.columns[i+1]).isNull(), 0).otherwise(F.col(total.columns[i+1])))
    total = \
    total.na.fill(0)
    # Null 값 제거 확인 로그 출력
    print('#',total[i+1],'에 있는 Null 값 개수 : ',total.filter(total[i+1].isNull()).count(),'\n')

# 조인된 데이터프레임 출력
#total.show()

#저장
total.write.csv('/user/ubuntu/data/merge_ratio.csv',header=True)

a = spark.read.csv('/user/ubuntu/data/merge_ratio.csv',header=True,inferSchema=True)
