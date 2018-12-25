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
    StructField("shortMessage", DataTypes.StringType, true)::
     StructField("author",StructType(
      StructField("name", DataTypes.StringType, true) :: Nil) )
      ::Nil)

  override def planInputPartitions():  util.List[InputPartition[InternalRow]] = {
    util.Arrays.asList(new GitInputPartition(options.get(DataSourceOptions.PATH_KEY).get))
  }

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
      val revCommit = log.next()
      InternalRow (str(revCommit.getShortMessage),
       InternalRow( str( revCommit.getAuthorIdent.getName)))
   }
   def str(string:String) = UTF8String.fromString(string)

   override def close(): Unit = {}
 }