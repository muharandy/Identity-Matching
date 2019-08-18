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


master = spark.sql('SELECT * FROM dwhdb.master_matching')
#master.limit(10).toPandas()

delta = spark.sql('SELECT * FROM dwhdb.delta_matching')
#delta.limit(10).toPandas()
    
master = (master
          .withColumn("clean_id",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('nomor_identitas', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
          .withColumn("clean_nama",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('nama_sesuai_identitas', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
          .withColumn("clean_tgl_lahir",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('tanggal_lahir', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
          .withColumn("clean_nama_ibu",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('nama_gadis_ibu_kandung', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
          .withColumn("clean_jenis_kelamin",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('jenis_kelamin', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
          .withColumn("clean_tempat_lahir",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('tempat_lahir', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
         )
#master.limit(10).toPandas()
master.registerTempTable("master")

prep_master = spark.sql("""
SELECT
    master.matching_id
    , master.clean_id
    , master.clean_nama
    , master.clean_tgl_lahir
    , master.clean_nama_ibu
    , master.clean_jenis_kelamin
    , master.clean_tempat_lahir
    , master.kode_pos
FROM master
  """)

prep_master.repartition("clean_tempat_lahir").write.format("parquet").partitionBy("clean_tempat_lahir").mode("overwrite").saveAsTable("dwhdb.fm_prep_master")

delta = (delta
          .withColumn("clean_id",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('nomor_identitas', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
          .withColumn("clean_nama",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('nama_sesuai_identitas', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
          .withColumn("clean_tgl_lahir",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('tanggal_lahir', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
          .withColumn("clean_nama_ibu",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('nama_gadis_ibu_kandung', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
         .withColumn("clean_jenis_kelamin",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('jenis_kelamin', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
         .withColumn("clean_tempat_lahir",
                      F.regexp_replace(F.trim(F.lower(F.regexp_replace('tempat_lahir', "[^a-zA-Z0-9\\s]", ""))), " +", " "))
         )
#delta.limit(10).toPandas()
delta.registerTempTable("delta")

prep_delta = spark.sql("""
SELECT
      delta.clean_id
    , delta.clean_nama
    , delta.clean_tgl_lahir
    , delta.clean_nama_ibu
    , delta.clean_jenis_kelamin
    , delta.clean_tempat_lahir
    , delta.kode_pos
FROM delta
  """)

prep_delta.repartition("clean_tempat_lahir").write.format("parquet").partitionBy("clean_tempat_lahir").mode("overwrite").saveAsTable("dwhdb.fm_prep_delta")
