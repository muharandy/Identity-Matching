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
master.registerTempTable("master")

delta_lv1 = spark.sql("SELECT * FROM dwhdb.fm_delta_lv1")
delta_lv1.registerTempTable("delta_lv1")

prep_delta_lv2 = spark.sql("""
SELECT /*+  BROADCASTJOIN(delta_lv1) */
    master.matching_id
    , delta_lv1.clean_id as d_clean_id
    , master.clean_nama as m_clean_nama
    , delta_lv1.clean_nama as d_clean_nama
    , delta_lv1.clean_tgl_lahir as d_clean_tgl_lahir
    , delta_lv1.clean_nama_ibu as d_clean_nama_ibu
    , delta_lv1.clean_jenis_kelamin as d_clean_jenis_kelamin
    , delta_lv1.kode_pos as d_kode_pos
    , delta_lv1.clean_tempat_lahir as d_clean_tempat_lahir
FROM master
INNER JOIN delta_lv1
ON master.clean_nama_ibu=delta_lv1.clean_nama_ibu
AND master.clean_tgl_lahir=delta_lv1.clean_tgl_lahir
""")

#prep_delta_lv2.write.format("parquet").partitionBy("m_clean_tempat_lahir").mode("overwrite").saveAsTable("dwhdb.fm_prep_delta_lv2")



#print(prep_delta_lv2.count())

match_lv2 = prep_delta_lv2.withColumn("similarity",get_similarity(prep_delta_lv2["m_clean_nama"],prep_delta_lv2["d_clean_nama"]))
match_lv2 = match_lv2.filter(match_lv2["similarity"] >= 80)

#print(match_lv2.count())
#print(match_lv2.limit(10).show())

#delta = spark.sql('SELECT * FROM dwhdb.fm_prep_delta')
delta_lv2 = delta_lv1.exceptAll(match_lv2
                                .drop('matching_id')
                                .drop('m_clean_nama')
                                .drop('similarity'))

#print(delta_lv2.count())
#print(delta_lv2.limit(10).show())

delta_lv2.write.format("parquet").partitionBy("clean_tempat_lahir").mode("overwrite").saveAsTable("dwhdb.fm_delta_lv2")

