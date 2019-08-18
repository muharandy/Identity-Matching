from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext, Row
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pyspark.sql.functions as F 
import time
from pyspark.sql.functions import lit

spark = (SparkSession
                .builder
                .appName('matching')
                .enableHiveSupport()
                .getOrCreate())

SparkContext.setSystemProperty("hive.metastore.uris", "http://192.168.58.24:8888")
spark.conf.set("spark.sql.crossJoin.enabled", "true")


master = spark.sql('SELECT * FROM dwhdb.fm_prep_master')
#master.limit(10).toPandas()

delta = spark.sql('SELECT * FROM dwhdb.fm_prep_delta')
#delta.limit(10).toPandas()

master.registerTempTable("master")

delta.registerTempTable("delta")

match_lv1 = spark.sql("""
SELECT /*+  BROADCASTJOIN(delta) */
	master.matching_id
    , delta.clean_id
    , delta.clean_nama
    , delta.clean_tgl_lahir
    , delta.clean_nama_ibu
    , delta.clean_jenis_kelamin
    , delta.kode_pos
    , delta.clean_tempat_lahir
FROM master
INNER JOIN delta
ON master.clean_id=delta.clean_id
AND master.clean_nama=delta.clean_nama
AND master.clean_tgl_lahir=delta.clean_tgl_lahir
""")

#print(match_lv1.count())

delta_lv1 = delta.exceptAll(match_lv1.drop('matching_id'))

#print(delta_lv1.count())

delta_lv1.write.format("parquet").partitionBy("clean_tempat_lahir").mode("overwrite").saveAsTable("dwhdb.fm_delta_lv1")