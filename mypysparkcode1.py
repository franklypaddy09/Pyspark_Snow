from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark =SparkSession.builder.master('local[*]').appName('test').getOrCreate()

data = "C:\\Bigdata\\drivers\\world_bank.json"
df = spark.read.format('json').load(data)
df.printSchema()
#df.show(truncate= False)
#df.show()


Res=(df.withColumn('_id_$oid',col('_id.$oid')).drop('_id')\
    .withColumn('majorsector_percent',explode(col('majorsector_percent'))).withColumn('majorsector_percent_Name',col('majorsector_percent.Name')).withColumn('majorsector_percent_Percent',col('majorsector_percent.Percent')).drop('majorsector_percent')\
    .withColumn('mjsector_namecode',explode(col('mjsector_namecode'))).withColumn('mjsector_namecode_code',col('mjsector_namecode.code')).withColumn('mjsector_namecode_name',col('mjsector_namecode.name')).drop('mjsector_namecode')\
    .withColumn('mjtheme',explode(col('mjtheme')))\
    .withColumn('mjtheme_namecode',explode(col('mjtheme_namecode'))).withColumn('mjtheme_namecode_code',col('mjtheme_namecode.code')).withColumn('mjtheme_namecode_name',col('mjtheme_namecode.name')).drop('mjtheme_namecode')\
    .withColumn('projectdocs',explode(col('projectdocs'))).withColumn('projectdocs_DocDate',col('projectdocs.DocDate')).withColumn('projectdocs_DocType',col('projectdocs.DocType')).withColumn('projectdocs_DocTypeDesc',col('projectdocs.DocTypeDesc')).withColumn('projectdocs_DocURL',col('projectdocs.DocURL')).withColumn('projectdocs_EntityID',col('projectdocs.EntityID')).drop('projectdocs')\
    .withColumn('sector',explode(col('sector'))).withColumn('sector_Name',col('sector.Name')).drop('sector')\
    .withColumn('sector_namecode',explode(col('sector_namecode'))).withColumn('sector_namecode_code',col('sector_namecode.code')).withColumn('sector_namecode_name',col('sector_namecode.name')).drop('sector_namecode')\
    .withColumn('theme_namecode',explode('theme_namecode')).withColumn('theme_namecode_code',col('theme_namecode.code')).withColumn('theme_namecode_name',col('theme_namecode.name')).drop('theme_namecode')\
    .withColumn('project_abstract_cdata',col('project_abstract.cdata')).drop('project_abstract')\
    .withColumn('sector1_Name',col('sector1.Name')).withColumn('sector1Percent',col('sector1.percent')).drop('sector1')\
    .withColumn('sector2_Name',col('sector2.Name')).withColumn('sector2_Percent',col('sector2.Percent')).drop('sector2')\
    .withColumn('sector3_Name',col('sector3.Name')).withColumn('sector3_Percent',col('sector3.Percent')).drop('sector3')\
    .withColumn('sector4_Name',col('sector4.Name')).withColumn('sector4_Percent',col('sector4.Percent')).drop('sector4')\
    .withColumn('theme1_Name',col('theme1.Name')).withColumn('theme1_Percent',col('theme1.Percent')).drop('theme1'))



res.printSchema()
res.show(truncate=False)

