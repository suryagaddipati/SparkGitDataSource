package git

import org.apache.spark.sql.DataFrameReader

object GitDataSource {

   class GitDataSource(s: DataFrameReader)  {
    def git(path: String): DataFrameReader = {
      Array("log","diff").map(t =>{
        val data = s.format("sg.spark.git.DefaultSource").option("type",t).load(path)
        data.createTempView(t)
      })
      s
    }
  }
  implicit def gitDataSource(s: DataFrameReader ) = new GitDataSource(s)

}