import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction

import org.apache.commons.math3.stat.descriptive.moment._ //Skewness, Mean

import org.rogach.scallop._ //CLI argument parsing

object AnalyzeDiff {
  def main(args: Array[String]) {
    //parse CLI arguments
    val conf = new Conf(args)

    val sc = new SparkContext
    sc.setLogLevel(conf.logLevel())

    val spark = SparkSession.builder.appName("AnalyzeDifference").getOrCreate
    val fileBasePath = conf.fileBasePath() //"/home/shen449/intel/vtune/projects/pc01-rapids/"

    //TODO: scan the directory to obtain a list of queries to process
    //TODO: for each query, figure out how many (=$iteration) files require processing

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
        .csv(fileBasePath + s"r027frame$idx.csv")

      //rename the columns so that no space character exists; all columns' name except the first column are added the suffix "_x" where x is ${idx}
      val original_columns = original_table.columns
      val renamed_columns = original_columns.map { str =>
        str match {
          case x if str.contains("Function") => x.replace(" ", "_")
          case _ => (str + s"_$idx").replace(" ", "_")
        }
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
        .select(
          "Function_(Full)",
          s"CPU_Time_${idx}"
        ) //, s"Instructions_Retired_${idx}")

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
        a.join(b, Seq("Function_(Full)"), "outer")
      }
      val column_names = joined_table_with_nulls.columns

      joined_table_with_nulls.na.fill(0, column_names)
    }

    val groupped_table = outer_joined_table.groupBy("Function_(Full)").sum()

    //create a ArrayType column (which contains all CPU time) and add to the table
    //the purpose of doing this is to make per-row calculation easier (e.g. calculate the average CPU_Time of each row)

    val table_with_array = {
      val cpu_time_column_names = (1 to iteration).map(x => s"sum(CPU_Time_$x)")
      val seq_of_cpu_time_columns: Seq[Column] =
        groupped_table.columns.foldLeft(Seq[Column]()) { (seq, cn) =>
          cn match {
            case x if cpu_time_column_names.contains(cn) =>
              seq :+ groupped_table.apply(x)
            case _ => seq
          }
        }
      groupped_table.withColumn(
        "CPU_Time_Array",
        array(seq_of_cpu_time_columns: _*)
      )
    }
    table_with_array.createOrReplaceTempView("table_with_array")
    //table_with_array.show()

    //calculate skewness of each row's CPU Time
    val skewness = udf((x: Seq[Double]) => {
      val sk = new Skewness()
      sk.evaluate(x.toArray, 0, x.length)
    })

    val avrg = udf((x: Seq[Double]) => {
      val mn = new Mean()
      mn.evaluate(x.toArray, 0, x.length)
    })

    spark.udf.register("skewness", skewness)
    spark.udf.register("avrg", avrg)

    val calculated_table =
      spark.sql(
        "select *, skewness(CPU_Time_Array) AS Skewness, avrg(CPU_Time_Array) AS Average from table_with_array"
      )

    //rearrange the columns for a better view
    val rearranged_table = {
      val rearranged_column_names = calculated_table.columns.sorted
      val rearranged_columns =
        rearranged_column_names.map(calculated_table(_))
      calculated_table.select(rearranged_columns: _*)
      //The function signature of [[select]] is (Columns* => Dataframe).
      //What does * in the function signature mean? What does :_* in the function invocation mean? Check this out: https://scala-lang.org/files/archive/spec/2.13/04-basic-declarations-and-definitions.html#repeated-parameters
    }

    //create a reference ${final_table} to the table that we want to save
    val final_table = rearranged_table.drop("CPU_Time_Array")

    //write the table to file
    //repartition the table down to one partition so that only one file is generated
    print(final_table.schema + "\n")
    final_table.show
    final_table
      .repartition(1)
      .write
      .options(Map("sep" -> "|||", "header" -> "True"))
      .mode("overwrite")
      .csv(fileBasePath + "r027joined_table.csv")

  }

}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val fileBasePath = opt[String](
    short = 'f',
    required = false,
    default = Option("/home/shen449/intel/vtune/projects/pc01-rapids/"),
    descr =
      """Where the home directory for vTune csv-format report files is located. This direcotry is required to have a hierarchy looking like this: 
                |fileBasePath
                |       ├─q1
                |       │  ├─it1
                |       │  ├─it2
                |       │  └─it3
                |       ├─q2
                |       │  ├─it1
                |       │  ├─it2
                |       │  └─it3
                |       └...""".stripMargin
  )
  val logLevel = opt[String](
    short = 'l',
    required = false,
    default = Option("WARN"),
    descr =
      "Control Spark's log level. Valid log levels include : ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN",
    validate =
      (Seq("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN")
        .contains(_))
  )
  val dryRun = opt[Boolean](
    short = 'n'
  )

  verify()
}
