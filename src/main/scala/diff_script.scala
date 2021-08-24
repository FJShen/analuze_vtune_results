import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction

import org.apache.commons.math3.stat.descriptive.moment._ //Skewness, Mean

import org.rogach.scallop._ //CLI argument parsing

import org.apache.commons.io._
import java.io.{File, FileFilter}
import java.lang.Exception

object AnalyzeDiff {
  def main(args: Array[String]) {
    //parse CLI arguments
    val conf = new Conf(args)

    val sc = new SparkContext
    sc.setLogLevel(conf.logLevel())

    val spark = SparkSession.builder.appName("AnalyzeDifference").getOrCreate

    spark.udf.register("skewness", skewness)
    spark.udf.register("avrg", avrg)

    //scan the directory to obtain a list of queries to process
    val reportLocation = conf.reportLocation()
    val dir = new File(reportLocation)
    val dir_content_list = dir.listFiles()
    print(
      "get dir_content_list from " + dir
        .getPath() + s", length is ${dir_content_list.length}\n"
    )
    dir_content_list.foreach { x =>
      print(x.getPath() + "\n")
    }

    dir_content_list.foreach { query_path =>
      analyzeQueryResult(spark, query_path)
    }
    return
  }

  def analyzeQueryResult(spark: SparkSession, query_path: File) {
    print(s"query path name is ${query_path.getName()}\n")

    lazy val dir_only_filter = new FileFilter(){
      override def accept(file: File): Boolean = {
        //ignore all directories in this folder
        !(file.isDirectory())
      }
    }

    val query_dir_content_list = query_path.listFiles(dir_only_filter)
    query_dir_content_list.foreach(x=>
      print(s"file name is ${x.getName()}\n")
    )

    if (query_dir_content_list.length == 0) {
      print(s"${query_path.getPath()} is void of files. Skipping this.\n")
    } else {
      //extract the number of iteration and find the maximum value
      val name_list = query_dir_content_list.map(_.getName())
      val csv_name_pattern = """it(\d+)\.csv""".r
      val stripped_name_list = name_list.map { x =>
        x match {
          case csv_name_pattern(value) => {
            print(s"value is $value\n")
            value
          }
          case _ => {
            throw new Exception(s"${x}: file format wrong")
          }
        }
      }

      val iteration: Int = stripped_name_list.max.toInt
      print(s"iteration is $iteration\n")

      if(query_path.getName()=="q1")
        createTable(spark, query_path, iteration)
    }
  }

  def createTable(spark: SparkSession, query_path: File, iteration: Int) {

    val show_and_write_csv = true
    val columns_of_interest: Seq[String] = Seq(
      "Function_(Full)",
      "CPU_Time",
      "CPU_Time:Effective_Time:Idle",
      "CPU_Time:Effective_Time:Poor",
      "CPU_Time:Effective_Time:Ok",
      "CPU_Time:Effective_Time:Ideal",
      "CPU_Time:Effective_Time:Over"
    )

    //the renaming function replaces all spaces with equal amount of underscores, 
    //and adds a numerical suffix to the string if the string does not contain substring "Function"
    def rename_column_function(idx: Int)(str: String) = {
      str match {
        case x if str.contains("Function") => x.replace(" ", "_")
        case _                             => (str + s"_$idx").replace(" ", "_")
      }
    }

    val table_list: Seq[DataFrame] = (1 to iteration).toList.map { idx =>
      //read the raw output file from vtune report
      val csv_file = new File(query_path, s"it${idx}.csv")
      //print(s"csv_file path is ${csv_file.getPath()}\n")

      val original_table = spark.read
        .options(
          Map(
            "sep" -> "|||",
            "header" -> "True",
            "inferSchema" -> "True",
            "enforceSchema" -> "False"
          )
        )
        .csv(csv_file.getPath())

      //rename the columns so that no space character exists; all columns' name except the first column are added the suffix "_x" where x is ${idx}
      val original_columns = original_table.columns
      val renamed_columns = original_columns.map(rename_column_function(idx))
      val renamed_table =
        (original_columns zip renamed_columns).foldLeft(original_table) {
          (tbl, pair) =>
            tbl.withColumnRenamed(pair._1, pair._2)
        }

      //only take record of function calls that exceed 0.05 CPU seconds
      //project the renamed table on only the columns we are interested
      val filter_criterion = s"CPU_Time_${idx}>0.05"      
      val renamed_columns_of_interest = columns_of_interest.map(rename_column_function(idx))
      val dummy_column = renamed_table("Function").as("Dummy")
      val filtered_table = renamed_table
        .withColumn("Dummy", dummy_column)
        .filter(filter_criterion)
        .select("Dummy", renamed_columns_of_interest:_*)
        .drop("Dummy")

      if (show_and_write_csv) {
        filtered_table.show() //Spark typically only shows the first twenty rows
        print(filtered_table.schema + "\n")
        /*filtered_table.write
          .options(Map("sep" -> "|||", "header" -> "True"))
          .mode("overwrite")
          .csv(reportLocation + s"frame${idx}_renamed.csv")*/
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

    //aggregate the rows by function name
    //this table is defined as a VAR because it will be clobbered when we add the array-type columns to it 
    var groupped_table = outer_joined_table.groupBy("Function_(Full)").sum()

    //create a ArrayType column (which contains all CPU time) and add to the table
    //the purpose of doing this is to make per-row calculation easier (e.g. calculate the average CPU_Time of each row)
    //Each column - let denote it 'x' - that we name in ${columns_of_interest} needs to be created an array that aggregates 
    //col(x_1), col(x_2), ..., col(x_n) where n is equal to ${iteration}

    /*val table_with_array = {
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
    }*/

    val table_with_array_2 = {
      columns_of_interest.foreach{ 
        case coi if (coi.contains("Function") == false) => {
          val per_iteration_column_names = (1 to iteration).map(idx => "sum(" + coi + s"_${idx})")
          val seq_of_columns : Seq[Column] = 
          groupped_table.columns.foldLeft(Seq[Column]()) { (seq, cn) =>
            cn match {
              case cn if per_iteration_column_names.contains(cn) =>
                seq :+ groupped_table.apply(cn)
              case _ => seq
            }
          }
          groupped_table = groupped_table.withColumn(coi + "_Array", array(seq_of_columns:_*))
        }
        case _ => {}
      }
      groupped_table
    }

    table_with_array_2.createOrReplaceTempView("table_with_array")
    //table_with_array_2.show()
    //table_with_array_2.printSchema()


    var sql_query_plan = Array[String]()
    columns_of_interest.foreach{
      case coi if(coi.contains("Function") == false) =>{
        val array_name = coi + "_Array"

        sql_query_plan = sql_query_plan :+ ","
        sql_query_plan = sql_query_plan :+ ("skewness(`" + array_name + "`) AS `Skewness_of_" + coi + "`" )

        sql_query_plan = sql_query_plan :+ ","
        sql_query_plan = sql_query_plan :+ ("avrg(`" + array_name + "`) AS `Average_of_" + coi +"`")
      }
      case _ => {}
    }
    //the sql query plan should now look like this: ", xxx, yyy, zzz, www, ..., aaa, bbb"
    //remove the first comma from the query plan
    sql_query_plan = sql_query_plan.tail

    //add the constant part to the query plan
    sql_query_plan = "select *," +: sql_query_plan
    sql_query_plan = sql_query_plan :+ "from table_with_array"
    
    val sql_query_plan_text = sql_query_plan.mkString(" ")
    print(sql_query_plan_text+"\n")

    val calculated_table =
      spark.sql(
        //"select *, skewness(CPU_Time_Array) AS Skewness_of_CPU_Time , avrg(CPU_Time_Array) AS Average_of_CPU_Time , skewness(CPU_Time:Effective_Time:Idle_Array) AS Skewness_of_CPU_Time:Effective_Time:Idle , avrg(CPU_Time:Effective_Time:Idle_Array) AS Average_of_CPU_Time:Effective_Time:Idle , skewness(CPU_Time:Effective_Time:Poor_Array) AS Skewness_of_CPU_Time:Effective_Time:Poor , avrg(CPU_Time:Effective_Time:Poor_Array) AS Average_of_CPU_Time:Effective_Time:Poor , skewness(CPU_Time:Effective_Time:Ok_Array) AS Skewness_of_CPU_Time:Effective_Time:Ok , avrg(CPU_Time:Effective_Time:Ok_Array) AS Average_of_CPU_Time:Effective_Time:Ok , skewness(CPU_Time:Effective_Time:Ideal_Array) AS Skewness_of_CPU_Time:Effective_Time:Ideal , avrg(CPU_Time:Effective_Time:Ideal_Array) AS Average_of_CPU_Time:Effective_Time:Ideal , skewness(CPU_Time:Effective_Time:Over_Array) AS Skewness_of_CPU_Time:Effective_Time:Over , avrg(CPU_Time:Effective_Time:Over_Array) AS Average_of_CPU_Time:Effective_Time:Over from table_with_array"
        //"select *, skewness(CPU_Time_Array) AS Skewness , avrg(CPU_Time_Array) AS Average from table_with_array"
        sql_query_plan_text
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
    //val final_table = rearranged_table.drop("CPU_Time_Array")
    val final_table = rearranged_table.columns.foldLeft(rearranged_table){(tbl, column_name) => 
      column_name match {
        case cn if column_name.contains("Array") => tbl.drop(cn)
        case _ => tbl
      }
    }

    //write the table to file
    //repartition the table down to one partition so that only one file is generated
    val result_file = new File(query_path, "result.csv")

    final_table.printSchema()
    //final_table.show
    final_table
      .repartition(1)
      .write
      .options(Map("sep" -> "|||", "header" -> "True"))
      .mode("overwrite")
      .csv(result_file.getPath())
  }

  val skewness = udf((x: Seq[Double]) => {
    val sk = new Skewness()
    sk.evaluate(x.toArray, 0, x.length)
  })

  val avrg = udf((x: Seq[Double]) => {
    val mn = new Mean()
    mn.evaluate(x.toArray, 0, x.length)
  })


}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val reportLocation = opt[String](
    short = 'r',
    required = true,
    //default = Option("/home/shen449/analyze_vtune_results/extracted_reports/"),
    descr =
      """Where the home directory for vTune csv-format report files is located. This direcotry is required to have a hierarchy looking like this: 
                |reportLocation
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
