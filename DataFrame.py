# Make a list of columns to drop 
cols_to_drop = ['FundingType', 'EstOutcome', 'PostYear', 'PostMonth']

# Drop the columns 
MatterMain_ToJoin4_7_2 = MatterMain_ToJoin4_7_2.drop(*cols_to_drop) 


#### Filter
# combining list into one
Matter_InoviceSum_Outliers = get_outliers(MatterMain_ToJoin4_7_2, 'Invoice_Sum (ThisMonth)', 250000)

MatterMain_ToJoin4_7_3= MatterMain_ToJoin4_7_2.filter(~MatterMain_ToJoin4_7_2.MainMatterID.isin(*Matter_InoviceSum_Outliers))
                #.filter(~MatterMain_ToJoin4_7_2.MainMatterID.isin(*Matter_EstDamagesSum_Outliers))\
                #.filter(~MatterMain_ToJoin4_7_2.MainMatterID.isin(*Matter_InoviceSum_Outliers))
    
#########################################################    
.filter((pf.col('Invoice_Sum (ThisMonth)')<250000) &\
#########################################################
        
Stages_Nulls_count_After_FF =  MatterMain_010_3.filter(MatterMain_010_3.StageID.isNull()).count()
        
# change the columns type       
 changedTypedf = joindf.withColumn("label", joindf["show"].cast(DoubleType()))
 changedTypedf = joindf.withColumn("label", joindf["show"].cast("double"))
 
 StringType
 
 changedTypedf = joindf.withColumn("show", col("show").cast("double"))

        
#### Append data frames:
columns = ['Variable', 'Correlation']
vals = [('',0.000)]

correlations_0 = spark.createDataFrame(vals, columns)
        
        
### Order
        from pyspark.sql.functions import desc

(group_by_dataframe
    .count()
    .filter("`count` >= 10")
    .sort(desc("count"))
 
 from pyspark.sql.functions import desc

(group_by_dataframe
    .count()
    .filter("`count` >= 10")
    .sort(desc("count"))
 
 correlations_0.orderBy('Correlation', ascending=False).take(10)
 
 

for i in numeric_variables:
    newRow = spark.createDataFrame([(i,MatterMain_model_train_lin_reg_1.stat.corr('Invoice_Sum (NextMonth)',i))]\
                                   , columns)
    correlations_0 = correlations_0.union(newRow)
 
 #########################################################################
 
from pyspark.sql.functions import col, isnan, when, trim

df = spark.createDataFrame([
    ("", 1, 2.0), ("foo", None, 3.0), ("bar", 1, float("NaN")), 
    ("good", 42, 42.0)])

def to_null(c):
    return when(~(col(c).isNull() | isnan(col(c)) | (trim(col(c)) == "")), col(c))


df.select([to_null(c).alias(c) for c in df.columns]).na.drop().show()
 
 #############################################################################################################
 highest_correlation_columns = [row.Variable for row in\
                               Correlations_1.orderBy('Correlation', ascending=False).select('Variable').take(10)]
 
