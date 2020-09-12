from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

csv_kt_file_name = "/user/ubuntu/kim/merge_ratio.csv"
# Loads data.
#dataset = spark.read.format("libsvm").load('a.txt')
kt = spark.read.csv(csv_kt_file_name,header=True,inferSchema=True)
# kt를 txt 파일로 변환하는 과정이 필요


# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)

# Make predictions
predictions = model.transform(dataset)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

####################
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
%matplotlib inline

csv_kt_file_name = "/user/ubuntu/data/merge_ratio.csv"
kt = spark.read.csv(csv_kt_file_name,header=True)

# 잘 불러와졌는지 확인
kt.head(1)

df = kt.toPandas() # 판다스로 변환

# 데이터프레임을 numpy array로 변환
data_points = df.values

kmeans = KMeans(n_clusters=3).fit(data_points)

# 각 점에 대한 클러스터 id 
kmeans.labels_

# 중심점
kmeans.cluster_centers_

df['cluset_id']=kmeans.labels_

df.head(2)

from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
df = spark.read.csv('/user/ubuntu/data/merge_ratio.csv', header=True, inferSchema=True)
new_df = df.withColumn('index', row_number().over(Window.orderBy(monotonically_increasing_id())))
new_df.write.csv('/user/ubuntu/kim/merge_ratio.csv',header=True)
