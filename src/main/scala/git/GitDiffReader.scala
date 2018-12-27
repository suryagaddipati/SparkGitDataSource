package git

import java.io.File
import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader, SupportsPushDownFilters}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.storage.file.FileRepositoryBuilder


class GitLogReader(options: DataSourceOptions) extends DataSourceReader with Logging with SupportsPushDownFilters  {

  override def readSchema(): StructType =  StructType(
    str("oldPath")::
    str("newPath")::
    str("oldMode")::
    str("newMode")::
    str("changeType")::
    str("oldSha")::
     str("newSha")::
      StructField("score", DataTypes.IntegerType, false)::
      Nil)

  override def planInputPartitions():  util.List[InputPartition[InternalRow]] = {
    util.Arrays.asList(new GitDiffPatitionReader(options.get(DataSourceOptions.PATH_KEY).get))
  }
 def str(fieldName: String)  = StructField(fieldName, DataTypes.StringType, false)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
      filters
  }

  override def pushedFilters(): Array[Filter] = {
   Array()
  }
}

class GitDiffPatitionReader(path: String) extends  InputPartition[InternalRow]{
  override def createPartitionReader(): GitLogInputReader =  {
    val repo = FileRepositoryBuilder.create(new File(path)).asInstanceOf[FileRepository]
    val git = new Git(repo)
    new GitLogInputReader(git.log().call().iterator())
  }
}
 class GitLogInputReader(log: util.Iterator[RevCommit]) extends InputPartitionReader[InternalRow]{
   override def next(): Boolean = log.hasNext


   override def get(): InternalRow = {
      val commit = log.next()
      InternalRow (
        s(commit.getId.name),s(commit.getId.abbreviate(7).name),
        s(commit.getFullMessage),s(commit.getShortMessage),
        commit.getCommitTime.toLong * 1000000,
        author(commit),
        committer(commit))
   }
   def author(commit :RevCommit):InternalRow = {
     commit.getTree
     val author = commit.getAuthorIdent
      InternalRow( s(author.getName), s(author.getEmailAddress))
   }
   def committer(commit :RevCommit):InternalRow = {
     val author = commit.getCommitterIdent
     InternalRow( s(author.getName), s(author.getEmailAddress))
   }
   def s(string:String) = UTF8String.fromString(string)

   override def close(): Unit = {}
 }