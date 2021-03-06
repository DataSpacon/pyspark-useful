import shutil
import os
import glob

# 1 indicates that I need one partition of csv only.

# please note that Folder has to have different name than file, to avoid removing by os.
shutil.rmtree('Matters_7',True)
Matters_7.repartition(1).write.option("inferSchema","true")\
                                        .option("timestampFormat","yyyy-MM-dd HH:mm:ss")\
.csv('Matters_7',header='true',timestampFormat = "yyyy-mm-dd hh:mm:ss")

# Get a list of all the file paths that ends with .txt from in specified directory
fileList=glob.glob('Matters_7/*.csv')
# Iterate over the list of filepaths & remove file.
for filePath in fileList:
    try:
        os.system("copy "+filePath+" Matters_7.csv")
    except:
        print("Error whilecopying file : ", filePath)
#remove whole temp folder
shutil.rmtree('Matters_7',True)
