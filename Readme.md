```scala
    val logFile = "/Users/sgaddipati/code/spark/.git" 
    val spark = SparkSession.builder
               .master("local[*]").appName("Git datasource").getOrCreate()
    val logData = spark.read.format("sg.spark.git").load(logFile)
    logData.createOrReplaceTempView("repoName")
    val m = spark.sql("
    select author.name, count(*)  from repoName group by author.name order by count(*) desc
    ")
    m.show()
    spark.stop()


|              name|count(1)|
+------------------+--------+
|     Matei Zaharia|    1592|
|       Reynold Xin|    1465|
|   Patrick Wendell|     857|
|       Wenchen Fan|     608|
|     Tathagata Das|     559|
|        Josh Rosen|     511|
|        Davies Liu|     469|
|         Sean Owen|     426|
|         Andrew Or|     393|
|       hyukjinkwon|     383|
|   Liang-Chi Hsieh|     365|
|        Cheng Lian|     355|
|    Marcelo Vanzin|     347|
|     Xiangrui Meng|     342|
|     Dongjoon Hyun|     323|
|        gatorsmile|     322|
|       Yanbo Liang|     291|
|Mosharaf Chowdhury|     290|
|      Shixiong Zhu|     284|
|  Michael Armbrust|     281|
+------------------+--------+
only showing top 20 rows

```


