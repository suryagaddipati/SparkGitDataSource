package sg.spark.git

import java.io.File
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.eclipse.jgit.api.{Git, LogCommand}
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.storage.file.FileRepositoryBuilder


class GitDataSourceReader(options: DataSourceOptions) extends DataSourceReader  {

  override def readSchema(): StructType =  StructType(
    StructField("shortMessage", DataTypes.StringType, true)::
      StructField("author", DataTypes.StringType, true)
      ::Nil)

  override def planInputPartitions():  util.List[InputPartition[InternalRow]] = {
     val x = new util.ArrayList[GitInputPartition]()
    x.add(new GitInputPartition(options.get(DataSourceOptions.PATH_KEY).get))
    x.asInstanceOf[util.List[InputPartition[InternalRow]]]
  }

//  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {}

//  override def getEndOffset: Offset = LongOffset(9l)
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
      InternalRow (UTF8String.fromString( revCommit.getShortMessage),UTF8String.fromString( revCommit.getAuthorIdent.getName))
   }

   override def close(): Unit = {}
 }