package sg.spark.git

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class DefaultSource extends DataSourceV2 with ReadSupport{
  override def createReader(options: DataSourceOptions): DataSourceReader =   options.get("type").get match{
    case "log" => new GitLogSourceReader(options)
    case _ => throw new IllegalArgumentException("Must provide type option")
  }

}

