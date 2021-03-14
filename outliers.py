from pyspark.sql.functions import udf

def get_outliers(df, column, limit):
    raw_df = df.filter(df[column]>limit)
    return raw_df.select('MainMatterID').rdd.flatMap(lambda x: x).collect()

spark.udf.register("get_outliers", get_outliers)
