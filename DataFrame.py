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
