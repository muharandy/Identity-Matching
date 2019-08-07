%AddJar file:/home/cdsw/lib/java-string-similarity-1.2.1.jar

import info.debatty.java.stringsimilarity._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()

//----------Load Data-----------
// Put code to load data here
val rawdata = spark.sql("SELECT * FROM staging.dummy")

//----------Explore Data-----------
// Put code to explore data here
display(rawdata)
rawdata.registerTempTable("rawdata");

//----------Cross Data-----------
// Put code to prepare cross data here
// Should apply filter to limit the product join, for example join on zip code or birthdate

val cross_data = spark.sql("""
 SELECT 
   A.cif as A_cif
   ,A.realname as A_realname
   ,A.mothername as A_mothername
   ,A.birthdate as A_birthdate
   ,B.cif as B_cif
   ,B.realname as B_realname
   ,B.mothername as B_mothername
   ,B.birthdate as B_birthdate
 FROM rawdata as A
 JOIN rawdata as B
   ON A.cif != B.cif 
""")

cross_data.registerTempTable("cross_data")

//----------Cleanse Data-----------
// Put code to prepare cross data here

// Sarjana
val gelar_sarjana = "(S\\.AB\\.|S\\.AP|S\\.ADM|S\\.AG|S\\.PD|S\\.AGR|S\\.P|S\\.ANT|S\\.ARS|S\\.DS|S\\.E\\.I|S\\.E|S\\.FARM|S\\.H\\.INT|S\\.HUM|S\\.H|S\\.GZ|S\\.KEL|S\\.IK|S\\.I\\.KOM|S\\.I\\.P|S\\.IP|S\\.IN|S\\.KED|S\\.KG|S\\.KH|S\\.HUT|S\\.KEB|S\\.KEP|S\\.K\\.M|S\\.KOM|S\\.KPM|S\\.MB|S\\.MAT|S\\.PAR|S\\.PD\\.I|S\\.PD\\.SD|S\\.PD|S\\.HAN|S\\.PT|S\\.PSI|S\\.SI|S\\.STP|S\\.SN|S\\.SI|S\\.SOS|S\\.SY|S\\.S|S\\.TI|S\\.T\\.P|S\\.TH\\.I|S\\.TH|S\\.TRK|S\\.T|S\\.|DRS\\.|DRS ||DR\\.|PROF\\.)".r

// Status Keluarga
val status_keluarga = "(PAK |BAPAK |IBU |BU |IBUNDA |BUNDA |NYONYA |NENEK |KAKEK |SAUDARA |SAUDARI |SDR |\\(ALM\\))".r

spark.udf.register("REMOVE_TITLE_DEGREE", (a: String) => gelar_sarjana.replaceAllIn(a,""))
spark.udf.register("REMOVE_TITLE_FAMILY", (a: String) => status_keluarga.replaceAllIn(a,""))
spark.udf.register("REMOVE_PUNCT", (a: String) => a.replaceAll("[^A-Z ]"," "))

val cleaned_data = spark.sql("""
SELECT
  REMOVE_PUNCT(REMOVE_TITLE_FAMILY(REMOVE_TITLE_DEGREE(upper(A_realname)))) as A_clean_name
  ,REMOVE_PUNCT(REMOVE_TITLE_FAMILY(REMOVE_TITLE_DEGREE(upper(A_mothername)))) as A_clean_mother
  ,REMOVE_PUNCT(REMOVE_TITLE_FAMILY(REMOVE_TITLE_DEGREE(upper(B_realname)))) as B_clean_name
  ,REMOVE_PUNCT(REMOVE_TITLE_FAMILY(REMOVE_TITLE_DEGREE(upper(B_mothername)))) as B_clean_mother
  ,TO_DATE(CAST(UNIX_TIMESTAMP(A_birthdate, 'dd/MM/yyyy') AS TIMESTAMP)) as A_birthdate_clean
  ,TO_DATE(CAST(UNIX_TIMESTAMP(B_birthdate, 'dd/MM/yyyy') AS TIMESTAMP)) as B_birthdate_clean
  ,*
FROM cross_data
""")

display(cleaned_data)
cleaned_data.registerTempTable("cleaned_data");

//----------Match Data-----------
// Put code to match cross data here

val jw = new JaroWinkler
val l =  new NormalizedLevenshtein
val d = new Damerau
val osa = new OptimalStringAlignment
val lcs = new LongestCommonSubsequence
val twogram = new NGram(2)
val dig = new QGram(2)
val cosine = new Cosine(2)

spark.udf.register("JAROWINKLER", (a: String,b:String ) => (1-jw.distance(a, b)))
spark.udf.register("NormalizedLevenshtein", (a: String,b:String ) => (l.distance(a, b)))
spark.udf.register("Damerau", (a: String,b:String ) => (d.distance(a, b)))
spark.udf.register("OptimalStringAlignment", (a: String,b:String ) => (osa.distance(a, b)))
spark.udf.register("LongestCommonSubsequence", (a: String,b:String ) => (lcs.distance(a, b)))
spark.udf.register("NGram", (a: String,b:String ) => (twogram.distance(a, b)))
spark.udf.register("QGram", (a: String,b:String ) => (dig.distance(a, b)))
spark.udf.register("Cosine", (a: String,b:String ) => (cosine.distance(a, b)))

spark.udf.register("NUMERIC_MATCH", (a: Int,b:Int ) => if(a==b) 1 else 0)
spark.udf.register("DATE_MATCH", (a:String,b:String ) => if(a==b) 1 else 0)

val result = spark.sql("""
  SELECT 
    *
    ,((JW_name * 2) + JW_mother + date_score )/4 AS SCORE 
  FROM(
    SELECT 
      A_realname
      ,B_realname
      ,A_mothername
      ,B_mothername
      ,A_birthdate
      ,B_birthdate
      ,JAROWINKLER(A_clean_name,B_clean_name) as JW_name
      ,JAROWINKLER(A_mothername,B_mothername) as JW_mother
      ,DATE_MATCH(CAST(A_birthdate_clean AS STRING), CAST(B_birthdate_clean AS STRING))as date_score
    FROM cleaned_data 
      )
  ORDER BY SCORE desc
  LIMIT 100
""")