package git

import java.io.File

import org.apache.spark.sql.SparkSession
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.diff.RawTextComparator
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.scalatest._
import GitDataSource._

class GitDataSourceReaderSpec extends FlatSpec with Matchers {
  "GitDataSource log" should "work with spark sql" in {
    val logFile = "/Users/sgaddipati/code/spark/.git" // Should be some file on your system
    val spark = SparkSession.builder.master("local[*]").appName("Git datasource").getOrCreate()
    spark.read.git(logFile)
    val sql =
      """
        |select * from log where shortSha ="4f17fdd" order by commitTime desc
      """.stripMargin
    val m = spark.sql(sql)
    m.show()
    spark.stop()
  }
  "GitDataSource diff" should "work with spark sql" in {
    val logFile = "/Users/sgaddipati/code/spark/.git" // Should be some file on your system
    val spark = SparkSession.builder.master("local[*]").appName("Git datasource").getOrCreate()
    spark.read.git(logFile)
//    val sql =
//      """
//        |select * from diff where oldSha ="827383a97c11a61661440ff86ce0c3382a2a23b2"  and newSha="0523f5e378e69f406104fabaf3ebe913de976bdb"
//      """.stripMargin
    val sql =
      """
        |select oldPath,oldSha,newSha from diff where oldPath="python/pyspark/sql/readwriter.py"
      """.stripMargin
    val m = spark.sql(sql)
    m.show(50,0,false)
    spark.stop()
  }
  "JGit" should "work with spark sql" in {
    val path = "/Users/sgaddipati/code/spark/.git" // Should be some file on your system
    val repo = FileRepositoryBuilder.create(new File(path)).asInstanceOf[FileRepository]
    val git = new Git(repo)
    val result = new Git(repo).blame().setFilePath("python/pyspark/sql/streaming.py")
      .setTextComparator(RawTextComparator.WS_IGNORE_ALL).call()
    val rawText = result.getResultContents()
    for ( i <- (0 to rawText.size()-1).zipWithIndex) {

         val sourceAuthor = result.getSourceAuthor(i._1);
       val sourceCommit = result.getSourceCommit(i._1);
      println(sourceAuthor.getName() +
        (if(sourceCommit != null)  "/" + sourceCommit.getCommitTime() + "/" + sourceCommit.getName() else "") +
        ": " + rawText.getString(i._1))
    }

    }
//    rawText.
//    for (int i = 0; i < rawText.size(); i++) {
//      System.out.println(sourceAuthor.getName() +
//        (sourceCommit != null ? "/" + sourceCommit.getCommitTime() + "/" + sourceCommit.getName() : "") +
//        ": " + rawText.getString(i));
//    }

}
