package git

import java.io.File
import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader, SupportsPushDownFilters}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.diff.DiffEntry
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.revwalk.{RevCommit, RevWalk}
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.eclipse.jgit.treewalk.AbstractTreeIterator


class GitDiffReader(options: DataSourceOptions) extends DataSourceReader with Logging with SupportsPushDownFilters  {
  var oldSha:String = null
  var newSha:String = null


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
    if(oldSha == null || newSha == null) throw  new IllegalArgumentException("diff query requires where clause with oldSha and newSha")
    util.Arrays.asList(new GitDiffPartitionReader(options.get(DataSourceOptions.PATH_KEY).get,oldSha,newSha))
  }
 def str(fieldName: String)  = StructField(fieldName, DataTypes.StringType, false)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {

    filters.filter(f => {
     var ret = true
     if(f.isInstanceOf[EqualTo]) {
       val eq :EqualTo =f.asInstanceOf[EqualTo]
       if(eq.attribute =="oldSha"){
         this.oldSha = eq.value.asInstanceOf[String]
          ret =false;
       }
       if(eq.attribute =="newSha"){
         this.newSha = eq.value.asInstanceOf[String]
         ret = false;
       }
     }
      ret
    })
  }

  override def pushedFilters(): Array[Filter] = {
   Array()
  }
}

class GitDiffPartitionReader(path: String, oldSha: String, newSha: String) extends  InputPartition[InternalRow]{

  def prepareTreeParser(repository: Repository, objectId: String): _root_.org.eclipse.jgit.treewalk.AbstractTreeIterator = {
    val walk = new RevWalk(repository)
    try {
    val commit = walk.parseCommit(repository.resolve(objectId))
    val tree = walk.parseTree(commit.getTree.getId)
    import org.eclipse.jgit.treewalk.CanonicalTreeParser
    val treeParser = new CanonicalTreeParser
      val reader = repository.newObjectReader
      try
        treeParser.reset(reader, tree.getId)
      finally if (reader != null) reader.close()
      treeParser
    }finally walk.dispose()
  }

  override def createPartitionReader(): GitDiffInputReader =  {
    val repo = FileRepositoryBuilder.create(new File(path)).asInstanceOf[FileRepository]
    val git = new Git(repo)
    val diffs = git.diff()
      .setOldTree(prepareTreeParser(repo, oldSha))
      .setNewTree(prepareTreeParser(repo, newSha))
      .call()
    new GitDiffInputReader(diffs.iterator,oldSha,newSha)
  }
}
 class GitDiffInputReader(log: util.Iterator[DiffEntry], oldSha: String, newSha: String) extends InputPartitionReader[InternalRow]{
   override def next(): Boolean = log.hasNext


   override def get(): InternalRow = {
      val diff = log.next()
      InternalRow (
        s(diff.getOldPath),s(diff.getNewPath),
        s(diff.getOldMode.toString),s(diff.getNewMode.toString),
        s(diff.getChangeType.toString),
        s(oldSha),s(newSha),
        diff.getScore
      )
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