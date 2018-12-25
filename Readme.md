```scala
    val logFile = "/Users/sgaddipati/code/spark/.git" 
   
    val logData = spark.read.format("sg.spark.git").load(logFile)
    logData.createOrReplaceTempView("repoName")
    spark.sql("
    select author.name, count(*)  from repoName group by author.name order by count(*) desc
    ").show



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

   spark.sql("
    select date_format(commitTime,'EEEE') as day, count(*)  from repoName group by date_format(commitTime,'EEEE') order by count(*) desc
    ").show
    
+---------+--------+
|      day|count(1)|
+---------+--------+
|  Tuesday|    4257|
|Wednesday|    4053|
| Thursday|    3920|
|   Monday|    3591|
|   Friday|    3417|
| Saturday|    1837|
|   Sunday|    1803|
+---------+--------+

```


