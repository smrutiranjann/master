from importing import *

d= [(1, 'biki mohanty'), (2, 'smruti ranjan'), (3, 'ranjan parida'), (4, 'avijit patt')]
df= spark.createDataFrame(d, ['id', 'name'])


# First Method
df1= df.withColumn('First Name', split(df['name'], ' ').getItem(0)) \
                .withColumn('Last Name', split(df['name'], ' ').getItem(1))
df1.show()

                    #OR
split_df= pyspark.sql.functions.split(df['name'], ' ')
df1= df.withColumn('First Name', split(df['name'], ' ').getItem(0)) \
                .withColumn('Last Name', split(df['name'], ' ').getItem(1))

                    #OR
import pyspark.sql.functions as funn
split_df= funn.split(df['name'], ' ')

df1= df.withColumn('First Name', split_df.getItem(0)).withColumn('Last Name', split_df.getItem(0))
df1.show()