import org.apache.spark.sql.SparkSession
import scopt.OParser

object AnalyzeDiff {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("AnalyzeDifference").getOrCreate
    val fileBasePath = "/home/shen449/intel/vtune/projects/pc01-rapids/"
    val iteration = 5

    (1 to iteration).foreach { idx =>
      val original_table = spark.read
        .options(Map("sep" -> "|||", "header" -> "True"))
        .csv(fileBasePath + s"frame$idx.csv")

      val original_columns = original_table.columns
      val renamed_columns = original_columns.head +: original_columns.tail.map {
        str => (str + s"_$idx").replace(" ","_")
      }
      val renamed_table =
        (original_columns zip renamed_columns).foldLeft(original_table) {
          (tbl, pair) =>
            tbl.withColumnRenamed(pair._1, pair._2)
        }
      val col_name = s"CPU_Time_${idx}"
      val filtered_table = renamed_table.filter($"col_name" > 0.5)
    
      renamed_table.show()
      renamed_table.write.options(Map("sep"->"|||", "header"->"True")).csv(fileBasePath + s"frame${idx}_renamed.csv")
    }

  }
}
