from importing import *
d= ([(1,'Biki', 'Bbsr', 'od', 26),
   (2,'Liki', 'blr', 'od', 28),
   (3,'Bibi', 'Chennai', 'ap', 32),
   (4,'Debu', 'Kendrapada', 'od', 25) ])
df= spark.createDataFrame(d, ['id', 'Name', 'Address', 'State', 'Age'])


# df.select(df.Name).show()
#
# df.select('id').show()
#
# df.select(df['Age']).show()
#
# df.select(col('Name')).show()
#
# df.select('*').show()


