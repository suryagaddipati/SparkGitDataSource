package git

import org.apache.spark.sql.SparkSession
import org.scalatest._

class GitDataSourceReaderSpec extends FlatSpec with Matchers {
  "GitDataSource" should "work with spark sql" in {
    val logFile = "/Users/sgaddipati/code/spark/.git" // Should be some file on your system
    val spark = SparkSession.builder.master("local[*]").appName("Git datasource").getOrCreate()
    val logData = spark.read.format("sg.spark.git").load(logFile)
    logData.createOrReplaceTempView("repoName")
    val m = spark.sql("select author.name, count(*)  from repoName group by author.name order by count(*) desc")
    m.show()
    spark.stop()
  }
}
