# 정답을 체크하지 않고 보기만 지우고 나간 경우에는 문제를 풀었다고 생각하기 어려움으로 제거함
csv_file_name = "/user/ubuntu/data/ednet_s6_v3.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, column
spark = SparkSession.builder.appName("").config("spark.some.config.option", "some-value").getOrCreate()

kt = spark.read.csv(csv_file_name,header=True,inferSchema=True)
kt = kt.orderBy(kt['index'])


##################################################################################
#1. [for문] enter와 submit timestamp 차를 이용하여 spent_time_list(time_list)를 생성 

spent_time_list = [] # 각 액션당 걸린 시간 리스트

for row in kt.collect() :
    if row.action_type == 'enter' :
        enter_time = row.timestamp
    elif (row.action_type == 'submit' or row.action_type=='quit'):
        spent_time_list.append(row.timestamp - enter_time)

###################################################################################################
#2. [for문] enter와 submit 사이에 time_list에 있는 값을 입력 -> 기존 df에 spent_time 열 생성 (df_time)

enter_flag=True # enter와 submit/quit 사이의 행인지 플래그
df_time_spent = [] # 한 문제에 푸는데 걸리는 시간 리스트
i = 0
for row in kt.collect() :
    if row.action_type == 'enter' :
        enter_flag = True
    elif (row.action_type == 'submit' or row.action_type=='quit'):
        i+=1
        enter_flag = False
    else :
        df_time_spent.append([row['index'],spent_time_list[i]])

df_time = spark.createDataFrame(df_time_spent, schema=['index','spent_time'])

# 기존 데이터프레임에 spent_time 조인
joined = kt.join(df_time,on=['index'],how='left_outer')

###################################################################################################
#3. [for문] enter와 submit 사이에 respond가 있는 액션의 첫 respond 행 추출(first_respond_index_df) 
#   enter와 submit 사이에 1개라도 있으면 continue
#   문제 진입하고 정답 respond한 행에 대한 index_list

respond_flag = False # enter와 submit/quit 사이의 행인지 플래그
first_respond_index = []

for row in kt.collect() :
    if row.action_type == 'enter' :
        respond_flag = False
    elif (row.action_type != 'submit') and (respond_flag == True) :
        continue #플래그 온 돼있으면 submit/quit 만날 때까지 모든 행 뛰어넘음
    elif (row.action_type == 'respond') : # enter와 submit/quit 사이에 respond가 나오면
        tmp_respond_row = row['index'] # 문제 진입하고 첫 정답 respond 행의 index를 임시 변수에 저장
        respond_flag = True # 플래그 온
    elif (row.action_type == 'submit' or row.action_type=='quit') :
        if respond_flag == True : # respond flag가 On이면 임시 변수의 index를 first_respond_index에 저장
            first_respond_index.append([tmp_respond_row])
        respond_flag = False

first_respond_index_df = spark.createDataFrame(first_respond_index,schema=['index'])

#4. joined와 first_respond_index_df 조인
result = joined.join(first_respond_index_df,on=['index'],how='inner').orderBy(['index'])

#5. 유저별 문제를 푸는데 걸린 평균 시간
mean_df = \
result.select('user_id','spent_time')\
.groupBy('user_id').mean()\
.select('user_id','avg(spent_time)')

# 저장
mean_df.write.csv('/user/ubuntu/data/avg_spent_time.csv',header=True)

"""
극단치를 제거했으나 분할한 파일에는 respond가 들어있지 않은 유저도 있음
kt.filter(kt.user_id==556666).show(200)
+--------+-------+-------------+-----------+-------+-----------+-------+-----------+--------+
|   index|user_id|    timestamp|action_type|item_id|cursor_time| source|user_answer|platform|
+--------+-------+-------------+-----------+-------+-----------+-------+-----------+--------+
|82695669| 556666|1574336291779|      enter|  e8234|       null|my_note|       null|  mobile|
|82695670| 556666|1574336293796| play_audio|  b8234|          0|my_note|       null|  mobile|
|82695671| 556666|1574336297654|pause_audio|  b8234|       3672|my_note|       null|  mobile|
|82695672| 556666|1574336314975| play_audio|  b8234|       3672|my_note|       null|  mobile|
|82695673| 556666|1574336326233|pause_audio|  b8234|      14998|my_note|       null|  mobile|
|82695674| 556666|1574336369257| play_audio|  b8234|          0|my_note|       null|  mobile|
|82695675| 556666|1574336372783|pause_audio|  b8234|       3426|my_note|       null|  mobile|
|82695676| 556666|1574336408427|       quit|  e8234|       null|my_note|       null|  mobile|
|82695677| 556666|1574336419689|      enter|  e5633|       null|my_note|       null|  mobile|
|82695678| 556666|1574336423211| play_audio|  b5633|          0|my_note|       null|  mobile|
|82695679| 556666|1574336427835|pause_audio|  b5633|       4581|my_note|       null|  mobile|
|82695680| 556666|1574336440248| play_audio|  b5633|       4581|my_note|       null|  mobile|
|82695681| 556666|1574336441168| play_audio|  b5633|       1092|my_note|       null|  mobile|
|82695682| 556666|1574336441168|pause_audio|  b5633|       5570|my_note|       null|  mobile|
|82695683| 556666|1574336444572|pause_audio|  b5633|       4364|my_note|       null|  mobile|
|82695684| 556666|1574336452036| play_audio|  b5633|       4364|my_note|       null|  mobile|
|82695685| 556666|1574336452634|pause_audio|  b5633|       5111|my_note|       null|  mobile|
|82695686| 556666|1574336452634| play_audio|  b5633|       1820|my_note|       null|  mobile|
|82695687| 556666|1574336455407|pause_audio|  b5633|       4457|my_note|       null|  mobile|
|82695688| 556666|1574336455408| play_audio|  b5633|       1520|my_note|       null|  mobile|
|82695689| 556666|1574336458667|pause_audio|  b5633|       4620|my_note|       null|  mobile|
|82695690| 556666|1574336458667| play_audio|  b5633|       1799|my_note|       null|  mobile|
|82695691| 556666|1574336461795| play_audio|  b5633|       1842|my_note|       null|  mobile|
|82695692| 556666|1574336461795|pause_audio|  b5633|       4775|my_note|       null|  mobile|
|82695693| 556666|1574336465104|pause_audio|  b5633|       4993|my_note|       null|  mobile|
|82695694| 556666|1574336474185| play_audio|  b5633|       4993|my_note|       null|  mobile|
|82695695| 556666|1574336478764|pause_audio|  b5633|       9757|my_note|       null|  mobile|
|82695696| 556666|1574336478765| play_audio|  b5633|       5654|my_note|       null|  mobile|
|82695697| 556666|1574336482804|pause_audio|  b5633|       9485|my_note|       null|  mobile|
|82695698| 556666|1574336492911| play_audio|  b5633|       6746|my_note|       null|  mobile|
|82695699| 556666|1574336495766|pause_audio|  b5633|       9471|my_note|       null|  mobile|
|82695700| 556666|1574336514257| play_audio|  b5633|       9471|my_note|       null|  mobile|
|82695701| 556666|1574336520429|pause_audio|  b5633|      15819|my_note|       null|  mobile|
|82695702| 556666|1574336520429| play_audio|  b5633|       9959|my_note|       null|  mobile|
|82695703| 556666|1574336525331|pause_audio|  b5633|      14749|my_note|       null|  mobile|
|82695704| 556666|1574336529473| play_audio|  b5633|      14749|my_note|       null|  mobile|
|82695705| 556666|1574336529765|pause_audio|  b5633|      15176|my_note|       null|  mobile|
|82695706| 556666|1574336529765| play_audio|  b5633|      10173|my_note|       null|  mobile|
|82695707| 556666|1574336535819|pause_audio|  b5633|      16121|my_note|       null|  mobile|
|82695708| 556666|1574336540011| play_audio|  b5633|      11608|my_note|       null|  mobile|
|82695709| 556666|1574336544521|pause_audio|  b5633|      16018|my_note|       null|  mobile|
|82695710| 556666|1574336578215| play_audio|  b5633|      16018|my_note|       null|  mobile|
|82695711| 556666|1574336582874|pause_audio|  b5633|      20828|my_note|       null|  mobile|
|82695712| 556666|1574336583508| play_audio|  b5633|      17092|my_note|       null|  mobile|
|82695713| 556666|1574336587147|pause_audio|  b5633|      20610|my_note|       null|  mobile|
|82695714| 556666|1574336595783| play_audio|  b5633|      13450|my_note|       null|  mobile|
|82695715| 556666|1574336601964|pause_audio|  b5633|      19496|my_note|       null|  mobile|
|82695716| 556666|1574336611166| play_audio|  b5633|      19498|my_note|       null|  mobile|
|82695717| 556666|1574336612311|pause_audio|  b5633|      20821|my_note|       null|  mobile|
|82695718| 556666|1574336627070|       quit|  e5633|       null|my_note|       null|  mobile|
|82695719| 556666|1574337839571|      enter| e11669|       null|my_note|       null|  mobile|
|82695720| 556666|1574337846115| play_audio| b11669|          0|my_note|       null|  mobile|
|82695721| 556666|1574337890103|pause_audio| b11669|      41039|my_note|       null|  mobile|
|82695722| 556666|1574339640883|       quit| e11669|       null|my_note|       null|  mobile|
|82695723| 556666|1574374504939|      enter| e11806|       null|my_note|       null|  mobile|
|82695724| 556666|1574374509513|       quit| e11806|       null|my_note|       null|  mobile|
|82695725| 556666|1574393290887|      enter|   l464|       null|archive|       null|  mobile|
|82695726| 556666|1574393304062| play_video|   l464|         35|archive|       null|  mobile|
|82695727| 556666|1574393307905|pause_video|   l464|       3272|archive|       null|  mobile|
|82695728| 556666|1574393309393| play_video|   l464|     225106|archive|       null|  mobile|
|82695729| 556666|1574393310825|pause_video|   l464|     226076|archive|       null|  mobile|
|82695730| 556666|1574393311309| play_video|   l464|         77|archive|       null|  mobile|
|82695731| 556666|1574393569294|       quit|   l464|       null|archive|       null|  mobile|
|82695732| 556666|1574393597700|      enter|  e8004|       null|my_note|       null|  mobile|
|82695733| 556666|1574393599310| play_audio|  b8004|          0|my_note|       null|  mobile|
|82695734| 556666|1574393603153|pause_audio|  b8004|       3620|my_note|       null|  mobile|
|82695735| 556666|1574393609821| play_audio|  b8004|       3620|my_note|       null|  mobile|
|82695736| 556666|1574393614009|pause_audio|  b8004|       7922|my_note|       null|  mobile|
|82695737| 556666|1574393622896| play_audio|  b8004|       7922|my_note|       null|  mobile|
|82695738| 556666|1574393627347|pause_audio|  b8004|      12399|my_note|       null|  mobile|
|82695739| 556666|1574393632399| play_audio|  b8004|      12399|my_note|       null|  mobile|
|82695740| 556666|1574393635992|pause_audio|  b8004|      16031|my_note|       null|  mobile|
|82695741| 556666|1574393657002| play_audio|  b8004|      16031|my_note|       null|  mobile|
|82695742| 556666|1574393657072|pause_audio|  b8004|      16102|my_note|       null|  mobile|
|82695743| 556666|1574393658224|       quit|  e8004|       null|my_note|       null|  mobile|
|82695744| 556666|1574393662254|      enter|   e181|       null|my_note|       null|  mobile|
|82695745| 556666|1574393712392| play_audio|   b181|          0|my_note|       null|  mobile|
|82695746| 556666|1574393728737|pause_audio|   b181|      16147|my_note|       null|  mobile|
|82695747| 556666|1574393737534| play_audio|   b181|      13435|my_note|       null|  mobile|
|82695748| 556666|1574393740231|pause_audio|   b181|      16111|my_note|       null|  mobile|
|82695749| 556666|1574393749135| play_audio|   b181|       9067|my_note|       null|  mobile|
|82695750| 556666|1574393752088|pause_audio|   b181|      12013|my_note|       null|  mobile|
|82695751| 556666|1574393752089| play_audio|   b181|       9317|my_note|       null|  mobile|
|82695752| 556666|1574393754547|pause_audio|   b181|      11640|my_note|       null|  mobile|
|82695753| 556666|1574393754548| play_audio|   b181|       8984|my_note|       null|  mobile|
|82695754| 556666|1574393759656|pause_audio|   b181|      13981|my_note|       null|  mobile|
|82695755| 556666|1574393763134|       quit|   e181|       null|my_note|       null|  mobile|
|82695756| 556666|1574393773195|      enter|   e176|       null|my_note|       null|  mobile|
|82695757| 556666|1574393774482| play_audio|   b176|          0|my_note|       null|  mobile|
|82695758| 556666|1574393792175|pause_audio|   b176|      17504|my_note|       null|  mobile|
|82695759| 556666|1574393813458| play_audio|   b176|          0|my_note|       null|  mobile|
|82695760| 556666|1574393831221|pause_audio|   b176|      17511|my_note|       null|  mobile|
|82695761| 556666|1574393834525|       quit|   e176|       null|my_note|       null|  mobile|
|82695762| 556666|1574393910011|      enter| e11669|       null|my_note|       null|  mobile|
|82695763| 556666|1574393913620|       quit| e11669|       null|my_note|       null|  mobile|
|82695764| 556666|1574393916706|      enter|  e1811|       null|my_note|       null|  mobile|
|82695765| 556666|1574393918554| play_audio|  b1811|          0|my_note|       null|  mobile|
|82695766| 556666|1574393959667|pause_audio|  b1811|      38087|my_note|       null|  mobile|
|82695767| 556666|1574393968459| play_audio|  b1811|       1045|my_note|       null|  mobile|
|82695768| 556666|1574394005742|pause_audio|  b1811|      38088|my_note|       null|  mobile|
|82695769| 556666|1574394011455|       quit|  e1811|       null|my_note|       null|  mobile|
|82695770| 556666|1574767217692|      enter|   e160|       null|my_note|       null|  mobile|
|82695771| 556666|1574767219092| play_audio|   b160|          0|my_note|       null|  mobile|
|82695772| 556666|1574767236556|pause_audio|   b160|      17319|my_note|       null|  mobile|
|82695773| 556666|1574767241450|       quit|   e160|       null|my_note|       null|  mobile|
|82695774| 556666|1574767250187|      enter|  e8027|       null|my_note|       null|  mobile|
|82695775| 556666|1574767251639| play_audio|  b8027|          0|my_note|       null|  mobile|
|82695776| 556666|1574767267252|pause_audio|  b8027|      15444|my_note|       null|  mobile|
|82695777| 556666|1574767268446|       quit|  e8027|       null|my_note|       null|  mobile|
|82695778| 556666|1574767273839|      enter|  e8234|       null|my_note|       null|  mobile|
|82695779| 556666|1574767274863| play_audio|  b8234|          0|my_note|       null|  mobile|
|82695780| 556666|1574767280856|pause_audio|  b8234|       5803|my_note|       null|  mobile|
|82695781| 556666|1574767280856| play_audio|  b8234|       1635|my_note|       null|  mobile|
|82695782| 556666|1574767283233|pause_audio|  b8234|       3831|my_note|       null|  mobile|
|82695783| 556666|1574767283233| play_audio|  b8234|       2067|my_note|       null|  mobile|
|82695784| 556666|1574767284660|pause_audio|  b8234|       3324|my_note|       null|  mobile|
|82695785| 556666|1574767284660| play_audio|  b8234|       1172|my_note|       null|  mobile|
|82695786| 556666|1574767298601|pause_audio|  b8234|      14995|my_note|       null|  mobile|
|82695787| 556666|1574767393986|       quit|  e8234|       null|my_note|       null|  mobile|
|82695788| 556666|1574767397696|      enter|  e8004|       null|my_note|       null|  mobile|
|82695789| 556666|1574767398672| play_audio|  b8004|          0|my_note|       null|  mobile|
|82695790| 556666|1574767414911|pause_audio|  b8004|      16099|my_note|       null|  mobile|
|82695791| 556666|1574767427182|       quit|  e8004|       null|my_note|       null|  mobile|
|82695792| 556666|1574767430687|      enter|  e5633|       null|my_note|       null|  mobile|
|82695793| 556666|1574767431904| play_audio|  b5633|          0|my_note|       null|  mobile|
|82695794| 556666|1574767436451|pause_audio|  b5633|       4370|my_note|       null|  mobile|
|82695795| 556666|1574767470401| play_audio|  b5633|       4370|my_note|       null|  mobile|
|82695796| 556666|1574767475514|pause_audio|  b5633|       9664|my_note|       null|  mobile|
|82695797| 556666|1574767475632| play_audio|  b5633|       9700|my_note|       null|  mobile|
|82695798| 556666|1574767476283|pause_audio|  b5633|      10319|my_note|       null|  mobile|
|82695799| 556666|1574767487043| play_audio|  b5633|       6853|my_note|       null|  mobile|
|82695800| 556666|1574767489035|pause_audio|  b5633|       8689|my_note|       null|  mobile|
|82695801| 556666|1574767492340| play_audio|  b5633|       5783|my_note|       null|  mobile|
|82695802| 556666|1574767496185|pause_audio|  b5633|       9466|my_note|       null|  mobile|
|82695803| 556666|1574767527660| play_audio|  b5633|       9466|my_note|       null|  mobile|
|82695804| 556666|1574767533558|pause_audio|  b5633|      15539|my_note|       null|  mobile|
|82695805| 556666|1574767546692| play_audio|  b5633|      12230|my_note|       null|  mobile|
|82695806| 556666|1574767549934|pause_audio|  b5633|      15342|my_note|       null|  mobile|
|82695807| 556666|1574767608980| play_audio|  b5633|      15342|my_note|       null|  mobile|
|82695808| 556666|1574767614321|pause_audio|  b5633|      20820|my_note|       null|  mobile|
|82695809| 556666|1574767614407|pause_audio|  b5633|      20820|my_note|       null|  mobile|
|82695810| 556666|1574767627495| play_audio|  b5633|      15228|my_note|       null|  mobile|
|82695811| 556666|1574767628905|pause_audio|  b5633|      16490|my_note|       null|  mobile|
|82695812| 556666|1574767628905| play_audio|  b5633|      13108|my_note|       null|  mobile|
|82695813| 556666|1574767634780|pause_audio|  b5633|      18817|my_note|       null|  mobile|
|82695814| 556666|1574767634781| play_audio|  b5633|      15721|my_note|       null|  mobile|
|82695815| 556666|1574767639143|pause_audio|  b5633|      19962|my_note|       null|  mobile|
|82695816| 556666|1574767639143| play_audio|  b5633|      15978|my_note|       null|  mobile|
|82695817| 556666|1574767644107|pause_audio|  b5633|      20828|my_note|       null|  mobile|
|82695818| 556666|1574767661423|       quit|  e5633|       null|my_note|       null|  mobile|
+--------+-------+-------------+-----------+-------+-----------+-------+-----------+--------+

"""