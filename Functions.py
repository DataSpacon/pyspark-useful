#UDF functions are applied on rows. They require to be registered as UDFs

def unpack_list(t):
    return list(set([item for sublist in t fro item in sublist]))

unpack_list_udf = pf.udf(unpack_list, T.ArrayType(T.FloatType()))

sub = sub.withColumn("Prices", unpack_list_udf(pf.col("Prices")))

