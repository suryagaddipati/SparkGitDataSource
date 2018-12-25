package sg.spark.git

import java.io.File
import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.storage.file.FileRepositoryBuilder


class GitDataSourceReader(options: DataSourceOptions) extends DataSourceReader with Logging  {

  override def readSchema(): StructType =  StructType(
    str("message")::
    str("shortMessage")::
      StructField("commitTime", DataTypes.TimestampType, false)::
    StructField("author", StructType(str("name")::str("email") :: Nil)) ::
    StructField("committer", StructType(str("name")::str("email") :: Nil)) ::
      Nil)

  override def planInputPartitions():  util.List[InputPartition[InternalRow]] = {
    util.Arrays.asList(new GitInputPartition(options.get(DataSourceOptions.PATH_KEY).get))
  }
 def str(fieldName: String)  = StructField(fieldName, DataTypes.StringType, false)
}
 class GitInputPartition(path: String) extends  InputPartition[InternalRow]{
   override def createPartitionReader(): GitLogReader =  {
     val repo = FileRepositoryBuilder.create(new File(path)).asInstanceOf[FileRepository]
     val git = new Git(repo)
     new GitLogReader(git.log().call().iterator())
   }
 }
 class GitLogReader(log: util.Iterator[RevCommit]) extends InputPartitionReader[InternalRow]{
   override def next(): Boolean = log.hasNext


   override def get(): InternalRow = {
      val commit = log.next()
      InternalRow (str(commit.getFullMessage),str(commit.getShortMessage), commit.getCommitTime.toLong * 1000000, author(commit),committer(commit))
   }
   def author(commit :RevCommit):InternalRow = {
     val author = commit.getAuthorIdent
      InternalRow( str(author.getName), str(author.getEmailAddress))
   }
   def committer(commit :RevCommit):InternalRow = {
     val author = commit.getCommitterIdent
     InternalRow( str(author.getName), str(author.getEmailAddress))
   }
   def str(string:String) = UTF8String.fromString(string)

   override def close(): Unit = {}
 }