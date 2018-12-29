package git

import org.apache.spark.sql.DataFrameReader

object GitDataSource {
  implicit class GitDataSource(val s: DataFrameReader) extends AnyVal  {
    def git(path: String) = {
      Array("log","diff").map(t =>{
        val data = s.format("sg.spark.git.DefaultSource").option("type",t).load(path)
        data.createTempView(t)
      })
    }
  }
}