import org.apache.spark.sql.{SparkSession, DataFrame}

import scopt.OParser
import org.apache.spark.SparkContext

object AnalyzeDiff {
  def main(args: Array[String]) {
    val sc = new SparkContext
    sc.setLogLevel("WARN")

    val spark = SparkSession.builder.appName("AnalyzeDifference").getOrCreate
    val fileBasePath = "/home/shen449/intel/vtune/projects/pc01-rapids/"
    val iteration = 5
    val show_and_write_csv = false

    val table_list: Seq[DataFrame] = (1 to iteration).toList.map { idx =>
      //read the raw output file from vtune
      val original_table = spark.read
        .options(Map("sep" -> "|||", "header" -> "True"))
        .csv(fileBasePath + s"frame$idx.csv")

      //rename the columns so that no space character exists; all columns' name except the first column are added the suffix "_x" where x is ${idx}
      val original_columns = original_table.columns
      val renamed_columns = original_columns.head +: original_columns.tail.map {
        str => (str + s"_$idx").replace(" ", "_")
      }
      val renamed_table =
        (original_columns zip renamed_columns).foldLeft(original_table) {
          (tbl, pair) =>
            tbl.withColumnRenamed(pair._1, pair._2)
        }

      //only take record of function calls that exceed 0.5 CPU seconds
      //project the renamed table on CPU Time and # of Insns Retired
      val filter_criterion = s"CPU_Time_${idx}>0.5"
      val filtered_table = renamed_table
        .filter(filter_criterion)
        .select("Function", s"CPU_Time_${idx}", s"Instructions_Retired_${idx}")

      if (show_and_write_csv) {
        filtered_table.show() //Spark typically only shows the first twenty rows
        filtered_table.write
          .options(Map("sep" -> "|||", "header" -> "True"))
          .mode("overwrite")
          .csv(fileBasePath + s"frame${idx}_renamed.csv")
      }

      //return value of "map"
      filtered_table
    }

    //start to join the ${iteration} tables
    val outer_joined_table = table_list.reduce { (a, b) =>
      a.join(b, Seq("Function"), "outer")
    }

    //write the table to file
    //repartition the table down to one partition so that only one file is generated
    outer_joined_table.show
    outer_joined_table.repartition(1).write
      .options(Map("sep" -> "|||", "header" -> "True"))
      .mode("overwrite")
      .csv(fileBasePath + "joined_table.csv")

  }
}
