package git

import java.io.File

import org.apache.spark.sql.SparkSession
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.scalatest._

class GitDataSourceReaderSpec extends FlatSpec with Matchers {
  "GitDataSource log" should "work with spark sql" in {
    val logFile = "/Users/sgaddipati/code/spark/.git" // Should be some file on your system
    val spark = SparkSession.builder.master("local[*]").appName("Git datasource").getOrCreate()
    val logData = spark.read.format("sg.spark.git.DefaultSource").option("type","log").load(logFile)
    logData.createTempView("logs")
    val sql =
      """
        |select * from logs where shortSha ="4f17fdd" order by commitTime desc
      """.stripMargin
    val m = spark.sql(sql)
    m.show()
    spark.stop()
  }
  "GitDataSource diff" should "work with spark sql" in {
    val logFile = "/Users/sgaddipati/code/spark/.git" // Should be some file on your system
    val spark = SparkSession.builder.master("local[*]").appName("Git datasource").getOrCreate()
    val logData = spark.read.format("sg.spark.git.DefaultSource").option("type","diff").load(logFile)
    logData.createTempView("diff")
//    val sql =
//      """
//        |select * from diff where oldSha ="827383a97c11a61661440ff86ce0c3382a2a23b2"  and newSha="0523f5e378e69f406104fabaf3ebe913de976bdb"
//      """.stripMargin
    val sql =
      """
        |select * from diff
      """.stripMargin
    val m = spark.sql(sql)
    m.show()
    spark.stop()
  }
  "JGit" should "work with spark sql" in {
    val path = "/Users/sgaddipati/code/spark/.git" // Should be some file on your system
    val repo = FileRepositoryBuilder.create(new File(path)).asInstanceOf[FileRepository]
    val git = new Git(repo)
    val c = git.log().call().iterator().next()
    println(c.getId)
  }
}
