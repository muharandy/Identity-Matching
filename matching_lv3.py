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

delta_lv2 = spark.sql("""
SELECT
    d_clean_id as clean_id
    , d_clean_nama as clean_nama
    , d_clean_tgl_lahir as clean_tgl_lahir
    , d_clean_nama_ibu as clean_nama_ibu
    , d_clean_jenis_kelamin as clean_jenis_kelamin
    , d_clean_tempat_lahir as clean_tempat_lahir
    , d_kode_pos as kode_pos
FROM dwhdb.fm_delta_lv2
""")

delta_lv2.registerTempTable("delta_lv2")

prep_delta_lv3 = spark.sql("""
SELECT /*+  BROADCASTJOIN(delta_lv2) */
    master.clean_id as m_clean_id
    , delta_lv2.clean_id as d_clean_id
    , master.clean_nama as m_clean_nama
    , delta_lv2.clean_nama as d_clean_nama
    , master.clean_tgl_lahir as m_clean_tgl_lahir
    , delta_lv2.clean_tgl_lahir as d_clean_tgl_lahir
    , master.clean_nama_ibu as m_clean_nama_ibu
    , delta_lv2.clean_nama_ibu as d_clean_nama_ibu
    , master.clean_jenis_kelamin as m_clean_jenis_kelamin
    , delta_lv2.clean_jenis_kelamin as d_clean_jenis_kelamin
    , master.clean_tempat_lahir as m_clean_tempat_lahir
    , delta_lv2.clean_tempat_lahir as d_clean_tempat_lahir
    , master.kode_pos as m_kode_pos
    , delta_lv2.kode_pos as d_kode_pos
FROM master
INNER JOIN delta_lv2
ON master.kode_pos=delta_lv2.kode_pos
""")

prep_delta_lv3 = (prep_delta_lv3
              .withColumn("similarity_nama",get_similarity(prep_delta_lv3["m_clean_nama"],prep_delta_lv3["d_clean_nama"]))
              .withColumn("similarity_nama_ibu",get_similarity(prep_delta_lv3["m_clean_nama_ibu"],prep_delta_lv3["d_clean_nama_ibu"]))
)

delta_lv3 = delta_lv3.filter(delta_lv3["similarity_nama"] >= 80).filter(delta_lv3["similarity_nama_ibu"] >= 80)

print(prep_delta_lv3.count())

#prep_delta_lv3.write.format("parquet").partitionBy("m_kode_pos").mode("overwrite").saveAsTable("dwhdb.fm_prep_delta_lv3")

#delta_lv3 = delta_lv3.filter(delta_lv3["similarity_nama"] >= 80).filter(delta_lv3["similarity_nama_ibu"] >= 80)

#delta_lv3.write.format("parquet").partitionBy("m_clean_tempat_lahir").mode("overwrite").saveAsTable("dwhdb.fm_delta_lv3")

