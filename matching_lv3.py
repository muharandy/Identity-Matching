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

def levenshtein_distance(word1, word2):
    """
    https://medium.com/@yash_agarwal2/soundex-and-levenshtein-distance-in-python-8b4b56542e9e
    https://en.wikipedia.org/wiki/Levenshtein_distance
    :param word1:
    :param word2:
    :return:
    """
    try:
        if (word1 is None) or (word2 is None):
            return 0.0

        word2 = word2.lower()
        word1 = word1.lower()
        matrix = [[0 for x in range(len(word2) + 1)] for x in range(len(word1) + 1)]

        for x in range(len(word1) + 1):
            matrix[x][0] = x
        for y in range(len(word2) + 1):
            matrix[0][y] = y

        for x in range(1, len(word1) + 1):
            for y in range(1, len(word2) + 1):
                if word1[x - 1] == word2[y - 1]:
                    matrix[x][y] = min(
                        matrix[x - 1][y] + 1,
                        matrix[x - 1][y - 1],
                        matrix[x][y - 1] + 1
                    )
                else:
                    matrix[x][y] = min(
                        matrix[x - 1][y] + 1,
                        matrix[x - 1][y - 1] + 1,
                        matrix[x][y - 1] + 1
                    )

        distance = matrix[len(word1)][len(word2)]
        max_ls = max([len(word1), len(word2)])
        if max_ls != 0:
            similarity = round(1-(float(distance)/max_ls), 4)*100
        else:
            similarity = 0.0
    except:
        return 0.0

    return similarity

get_similarity = F.udf(levenshtein_distance, DoubleType())

master = spark.sql('SELECT * FROM dwhdb.fm_prep_master')
master = master.repartition(2000)
master.registerTempTable("master")

delta_lv2 = spark.sql("SELECT * FROM dwhdb.fm_delta_lv2")
delta_lv2 = delta_lv2.repartition(2000)
delta_lv2.registerTempTable("delta_lv2")

prep_delta_lv3 = spark.sql("""
SELECT /*+  BROADCASTJOIN(delta_lv2) */
    master.matching_id
    , delta_lv2.clean_id as d_clean_id
    , master.clean_nama as m_clean_nama
    , delta_lv2.clean_nama as d_clean_nama
    , delta_lv2.clean_tgl_lahir as d_clean_tgl_lahir
    , master.clean_nama_ibu as m_clean_nama_ibu
    , delta_lv2.clean_nama_ibu as d_clean_nama_ibu
    , delta_lv2.clean_jenis_kelamin as d_clean_jenis_kelamin
    , delta_lv2.kode_pos as d_kode_pos
    , delta_lv2.clean_tempat_lahir as d_clean_tempat_lahir 
FROM master
INNER JOIN delta_lv2
ON master.kode_pos=delta_lv2.kode_pos
""")

prep_delta_lv3 = (prep_delta_lv3
              .withColumn("similarity_nama",get_similarity(prep_delta_lv3["m_clean_nama"],prep_delta_lv3["d_clean_nama"]))
              .withColumn("similarity_nama_ibu",get_similarity(prep_delta_lv3["m_clean_nama_ibu"],prep_delta_lv3["d_clean_nama_ibu"]))
)

match_lv3 = prep_delta_lv3.filter(prep_delta_lv3["similarity_nama"] >= 80).filter(prep_delta_lv3["similarity_nama_ibu"] >= 80)

print(match_lv3.count())
#print(match_lv3.limit(10).show())

#delta = spark.sql('SELECT * FROM dwhdb.fm_prep_delta')
delta_lv3 = delta_lv2.exceptAll(match_lv3
                                .drop('matching_id')
                                .drop('m_clean_nama')
                                .drop('m_clean_nama_ibu')
                                .drop('similarity_nama')
                                .drop('similarity_nama_ibu'))

#print(delta_lv3.count())
#print(delta_lv3.limit(10).show())

#print(prep_delta_lv3.count())

#prep_delta_lv3.write.format("parquet").partitionBy("m_kode_pos").mode("overwrite").saveAsTable("dwhdb.fm_prep_delta_lv3")

#delta_lv3 = delta_lv3.filter(delta_lv3["similarity_nama"] >= 80).filter(delta_lv3["similarity_nama_ibu"] >= 80)

#delta_lv3.write.format("parquet").partitionBy("lean_tempat_lahir").mode("overwrite").saveAsTable("dwhdb.fm_delta_lv3")

