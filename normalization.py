# 1
from sklearn.preprocessing import normalize

x = [1,2,3,4]
normalize([x]) # array([[0.18257419, 0.36514837, 0.54772256, 0.73029674]])
normalize([x], norm="l1") # array([[0.1, 0.2, 0.3, 0.4]])
normalize([x], norm="max") # array([[0.25, 0.5 , 0.75, 1.]])

#2

df = spark.createDataFrame([ (1, 'A',12560,45),
                             (1, 'B',42560,90),
                             (1, 'C',31285,120),
                             (1, 'D',10345,150)
                           ], ["userID", "Name","Income","No_of_Days"])

print("Before Scaling :")
df.show(5)

from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# UDF for converting column type from vector to double type
unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())

# Iterating over columns to be scaled
for i in ["Income","No_of_Days"]:
    # VectorAssembler Transformation - Converting column to vector type
    assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")

    # MinMaxScaler Transformation
    scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")

    # Pipeline of VectorAssembler and MinMaxScaler
    pipeline = Pipeline(stages=[assembler, scaler])

    # Fitting pipeline on dataframe
    df = pipeline.fit(df).transform(df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")

print("After Scaling :")
df.show(5)


# 3 keeping the same column name


# Iterating over columns to be scaled
for i in ["Client_Age","WIP_Sum (ThisMonth)"]:
    # VectorAssembler Transformation - Converting column to vector type
    assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")

    # MinMaxScaler Transformation
    scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")

    # Pipeline of VectorAssembler and MinMaxScaler
    pipeline = Pipeline(stages=[assembler, scaler])

    # Fitting pipeline on dataframe
    MatterMain_Nulls_4 = pipeline.fit(MatterMain_Nulls_4).transform(MatterMain_Nulls_4)\
                        .withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(*[i,i+"_Vect"])
    MatterMain_Nulls_4=MatterMain_Nulls_4.withColumnRenamed(i+"_Scaled",i)
