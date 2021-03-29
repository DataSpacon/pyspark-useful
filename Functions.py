# function to create list
def unpack_list(t):
    return list(set([item for sublist in t fro item in sublist]))

unpack_list_udf = pf.udf(unpack_list, T.ArrayType(T.FloatType()))

# now use that list to  convert 
sub = sub.withColumn("Prices", unpack_list_udf(pf.col("Prices")))


# get the mean based on the abovce functions
mean_udf = pf.udf(lambda x: float(np.mean(x)),T.FloatType())
median_udf = pf.udf(lambda x: float(np.median(x)), T.FloatType())

sub = (
    sub.withColumn("Mean", mean_udf("Prices"))
    .withColumn("Median", median_udf("Prices"))
    .drop("Prices")
)

# Clean some column names. Left is pandas, some change required for pyspark
housing.columns = [x.lower().replace(".","").replace(" ", "_") for x in housing.columns]
