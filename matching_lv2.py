from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext, Row
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pyspark.sql.functions as F 
import time
from pyspark.sql.functions import lit
from xmatchlib import levenshtein_distance

spark = (SparkSession
                .builder
                .appName('matching')
                .enableHiveSupport()
                .getOrCreate())

SparkContext.setSystemProperty("hive.metastore.uris", "http://192.168.58.24:8888")
spark.conf.set("spark.sql.crossJoin.enabled", "true")

get_similarity = F.udf(levenshtein_distance, DoubleType())

master = spark.sql('SELECT * FROM dwhdb.fm_prep_master')
master.registerTempTable("master")

delta_lv1 = spark.sql("SELECT * FROM dwhdb.fm_delta_lv1")
delta_lv1.registerTempTable("delta_lv1")

prep_delta_lv2 = spark.sql("""
SELECT /*+  BROADCASTJOIN(delta_lv1) */
    master.clean_id as m_clean_id
    , delta_lv1.clean_id as d_clean_id
    , master.clean_nama as m_clean_nama
    , delta_lv1.clean_nama as d_clean_nama
    , master.clean_tgl_lahir as m_clean_tgl_lahir
    , delta_lv1.clean_tgl_lahir as d_clean_tgl_lahir
    , master.clean_nama_ibu as m_clean_nama_ibu
    , delta_lv1.clean_nama_ibu as d_clean_nama_ibu
    , master.clean_jenis_kelamin as m_clean_jenis_kelamin
    , delta_lv1.clean_jenis_kelamin as d_clean_jenis_kelamin
    , master.clean_tempat_lahir as m_clean_tempat_lahir
    , delta_lv1.clean_tempat_lahir as d_clean_tempat_lahir
    , master.kode_pos as m_kode_pos
    , delta_lv1.kode_pos as d_kode_pos
FROM master
INNER JOIN delta_lv1
ON master.clean_nama_ibu=delta_lv1.clean_nama_ibu
AND master.clean_tgl_lahir=delta_lv1.clean_tgl_lahir
""")

#prep_delta_lv2.write.format("parquet").partitionBy("m_clean_tempat_lahir").mode("overwrite").saveAsTable("dwhdb.fm_prep_delta_lv2")



#print(prep_delta_lv2.count())

delta_lv2 = prep_delta_lv2.withColumn("similarity",get_similarity(prep_delta_lv2["m_clean_nama"],prep_delta_lv2["d_clean_nama"]))
delta_lv2 = delta_lv2.filter(delta_lv2["similarity"] >= 80)

delta_lv2.write.format("parquet").partitionBy("m_kode_pos").mode("overwrite").saveAsTable("dwhdb.fm_delta_lv2")









