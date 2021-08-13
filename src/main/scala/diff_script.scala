import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions.udf

import scopt.OParser
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction

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
        .options(
          Map(
            "sep" -> "|||",
            "header" -> "True",
            "inferSchema" -> "True",
            "enforceSchema" -> "False"
          )
        )
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

      //only take record of function calls that exceed 0.05 CPU seconds
      //project the renamed table on CPU Time and # of Insns Retired
      val filter_criterion = s"CPU_Time_${idx}>0.05"
      val filtered_table = renamed_table
        .filter(filter_criterion)
        .select("Function", s"CPU_Time_${idx}", s"Instructions_Retired_${idx}")

      if (show_and_write_csv) {
        filtered_table.show() //Spark typically only shows the first twenty rows
        print(filtered_table.schema + "\n")
        filtered_table.write
          .options(Map("sep" -> "|||", "header" -> "True"))
          .mode("overwrite")
          .csv(fileBasePath + s"frame${idx}_renamed.csv")
      }

      //return value of "map"
      filtered_table
    }

    //start to join the ${iteration} tables, null values are replaced with 0
    val outer_joined_table = {
      val joined_table_with_nulls = table_list.reduce { (a, b) =>
        a.join(b, Seq("Function"), "outer")
      }
      val column_names = joined_table_with_nulls.columns

      joined_table_with_nulls.na.fill(0, column_names)
    }

    //rearrange the columns for a better view
    val rearranged_table = {
      val rearranged_column_names = outer_joined_table.columns.sorted
      val rearranged_columns =
        rearranged_column_names.map(outer_joined_table(_))
      outer_joined_table.select(rearranged_columns: _*) 
      //The function signature of select is (Columns* => Dataframe). 
      //What does * in the function signature mean? What does :_* in the function invocation mean? Check this out: https://scala-lang.org/files/archive/spec/2.13/04-basic-declarations-and-definitions.html#repeated-parameters
    }

    //create a reference ${final_table} to the table that we want to save
    val final_table = rearranged_table

    //write the table to file
    //repartition the table down to one partition so that only one file is generated
    print(final_table.schema + "\n")
    final_table.show
    final_table
      .repartition(1)
      .write
      .options(Map("sep" -> "|||", "header" -> "True"))
      .mode("overwrite")
      .csv(fileBasePath + "joined_table.csv")

  }
}
