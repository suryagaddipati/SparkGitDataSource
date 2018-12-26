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


/// top 3 by day

val sq = """
SELECT
  name,
  day,
  commitCount
FROM (

SELECT author.name as name, date_format(commitTime,'EEEE') as day, count(*) as commitCount, dense_rank() OVER (PARTITION BY date_format(commitTime,'EEEE') ORDER BY count(*) DESC) as rank
  from repoName group by day,name) tmp
WHERE
  rank <= 3
  """
  
 spark.sql(sq).show
 
 +---------------+---------+-----------+
|           name|      day|commitCount|
+---------------+---------+-----------+
|  Matei Zaharia|   Friday|        224|
|    Reynold Xin|   Friday|        196|
|Patrick Wendell|   Friday|         95|
|  Matei Zaharia|   Monday|        238|
|    Reynold Xin|   Monday|        199|
|Patrick Wendell|   Monday|        160|
|  Matei Zaharia| Saturday|        207|
|    Reynold Xin| Saturday|        157|
|Patrick Wendell| Saturday|        107|
|  Matei Zaharia|   Sunday|        254|
|    Reynold Xin|   Sunday|        142|
|Patrick Wendell|   Sunday|         95|
|    Reynold Xin| Thursday|        252|
|  Matei Zaharia| Thursday|        206|
|Patrick Wendell| Thursday|        139|
|    Reynold Xin|  Tuesday|        247|
|  Matei Zaharia|  Tuesday|        239|
|Patrick Wendell|  Tuesday|        124|
|    Reynold Xin|Wednesday|        272|
|  Matei Zaharia|Wednesday|        224|
+---------------+---------+-----------+

```


