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
  lazy val sc = new SparkContext
  lazy val spark = SparkSession.builder.appName("AnalyzeDifference").getOrCreate

  val columns_of_interest: Seq[String] = Seq(
    "Function_(Full)", //this one is a must
    "CPU_Time",
    "CPU_Time:Effective_Time:Idle",
    "CPU_Time:Effective_Time:Poor",
    "CPU_Time:Effective_Time:Ok",
    "CPU_Time:Effective_Time:Ideal",
    "CPU_Time:Effective_Time:Over"
  )

  lazy val can_calculate_weighted_wall_time =
    columns_of_interest.contains("CPU_Time:Effective_Time:Poor") &&
      columns_of_interest.contains("CPU_Time:Effective_Time:Ok") &&
      columns_of_interest.contains("CPU_Time:Effective_Time:Ideal") &&
      columns_of_interest.contains("CPU_Time:Effective_Time:Over")

  val denominator = 40
  val weights = Map[String, Double](
    "Poor" -> 2,
    "Ok" -> 6,
    "Ideal" -> 10,
    "Over" -> 20
  )

  def main(args: Array[String]) {
    //parse CLI arguments
    val conf = new Conf(args)

    sc.setLogLevel(conf.logLevel())

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

    val il = conf.include_list()
    val queries_to_process: Array[String] = {
      if (il.length > 0) {
        il.toArray
      } else {
        dir_content_list.map(_.getName())
      }
    }
    print("Quries to process: ")
    queries_to_process.foreach(x => print(s"$x "))
    print("\n")

    print(s"Demand wall time is ${conf.demand_wall_time()}\n")
    print(s"drop_tiered_cpu_time is ${conf.drop_tiered_cpu_time()}\n")

    //do this for each query
    dir_content_list.foreach { query_path =>
      analyzeQueryResult(
        query_path,
        queries_to_process,
        !(conf.dryRun()),
        conf.demand_wall_time(),
        conf.drop_tiered_cpu_time()
      )
    }
    return
  }

  def analyzeQueryResult(
      query_path: File,
      queries_to_process: Array[String],
      save_result: Boolean = true,
      demand_wall_time: Boolean = false,
      drop_tiered_cpu_time: Boolean = false
  ) {
    //the report files' path will look like this: <report_location>/q1/test2/it1.csv
    if (!queries_to_process.contains(query_path.getName())) return

    print(s"Analyzing query report for ${query_path.getName()}\n")

    val file_only_filter = new FileFilter() {
      override def accept(file: File): Boolean = {
        //ignore all directories in this folder
        !(file.isDirectory())
      }
    }

    //todo: find out how many tests
    val test_name_pattern = """test(\d+)""".r
    val test_dir_list = query_path.listFiles()
    val test_dir_id_list = test_dir_list.map(_.getName()).map{
      case test_name_pattern(value) => value.toInt
      case _ => 0
    }
    val test_count: Int = test_dir_id_list.max


    //todo: find out how many iterations 
    //we will only probe the first dir (i.e. "test1/")
    val csv_name_pattern = """it(\d+)\.csv""".r
    val query_dir_list = test_dir_list.find(x=>x.getName().contains("test1")).get.listFiles(file_only_filter) //only accept files, not dirs
    val query_dir_id_list = query_dir_list.map(_.getName()).map{
      case csv_name_pattern(value) => value.toInt
      case _ => 0
    }

    val iteration_count: Int = query_dir_id_list.max

    print(s"testcount is $test_count, iterationcount is $iteration_count\n")
    test_dir_list.foreach(x => print(s"testdirlist: ${x.getName()}\n"))
    query_dir_list.foreach(x => print(s"querydirlist: ${x.getName()}\n"))


    val dataframe_matrix = Array.tabulate[DataFrame](test_count, iteration_count)(get_dataframe(query_path, demand_wall_time, drop_tiered_cpu_time))

    /*
    //this is test 3, iteration 3
    dataframe_matrix(2)(2).printSchema()
    dataframe_matrix(2)(2).show()
    dataframe_matrix(2)(2).coalesce(1)
        .write
        .options(Map("sep" -> "|||", "header" -> "True"))
        .mode("overwrite")
        .csv(new File(query_path, "result.csv").getPath())
    */

    //todo: for each iteration, join all their tests and find the median value for each metric
    val table_it1 = dataframe_matrix.tail.foldLeft(dataframe_matrix(0)(0)){ (df, ar_df) => 
      val joined_df = df.join(ar_df(0), Seq("Function_(Full)"), "inner")
      joined_df
    }

    //val table_it1_cputime = table_it1.select("CPU_Time_1")

    val table_to_print = table_it1
    table_to_print.printSchema()
    table_to_print.show(numRows=3, truncate=0, vertical = true)


    
  }

  def createTable(
      query_path: File,
      iteration: Int,
      save_result: Boolean = true,
      demand_wall_time: Boolean = false,
      drop_tiered_cpu_time: Boolean = false
  ) {

    val show_csv = false


    if (demand_wall_time && !can_calculate_weighted_wall_time) {
      throw new Exception(
        "CLI demanded wall time approximation be calculated; but input data does not contain all necessary data."
      )
    }

    val columns_eligible_to_create_an_array_for =
      if (can_calculate_weighted_wall_time && demand_wall_time)
        columns_of_interest :+ "Wall_Time_Approx"
      else columns_of_interest


    val table_list: Seq[DataFrame] = (1 to iteration).toList.map { idx =>
      //read the raw output file from vtune report
      val csv_file = new File(query_path, s"it${idx}.csv")

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
      //project the renamed table on only the columns we are interested in
      //val filter_criterion = s"CPU_Time_${idx}>0.05"
      val renamed_columns_of_interest =
        columns_of_interest.map(rename_column_function(idx))
      val dummy_column = renamed_table("Function").as("Dummy")
      val filtered_table = renamed_table
        .withColumn("Dummy", dummy_column)
        //.filter(filter_criterion)
        .select("Dummy", renamed_columns_of_interest: _*)
        .drop("Dummy")

      //aggregate the table
      val agg_table = filtered_table.groupBy("Function_(Full)").sum()

      //rename the columns where a "sum" has been put before the metric name
      val sum_name_pattern = """sum\((.*)\)""".r
      val agg_table_renamed = agg_table.columns.foldLeft(agg_table) {
        (tbl, cn) =>
          cn match {
            case sum_name_pattern(pure_name) =>
              tbl.withColumnRenamed(cn, pure_name)
            case _ => tbl
          }
      }


      //calculate a weighted average of the CPU effective time and call it the "wall time"
      //this is a wild approximation
      val final_per_iteration_table = {
        if (can_calculate_weighted_wall_time && demand_wall_time) {
          val weighted_column =
            agg_table_renamed(s"CPU_Time:Effective_Time:Poor_${idx}") / weights(
              "Poor"
            ) +
              agg_table_renamed(s"CPU_Time:Effective_Time:Ok_${idx}") / weights(
                "Ok"
              ) +
              agg_table_renamed(
                s"CPU_Time:Effective_Time:Ideal_${idx}"
              ) / weights(
                "Ideal"
              ) +
              agg_table_renamed(
                s"CPU_Time:Effective_Time:Over_${idx}"
              ) / weights(
                "Over"
              )
          agg_table_renamed.withColumn(
            s"Wall_Time_Approx_${idx}",
            weighted_column
          )
        } else agg_table_renamed
      }

      if (show_csv) {
        final_per_iteration_table
          .show() //Spark typically only shows the first twenty rows
        print(final_per_iteration_table.schema + "\n")
      }

      //return value of "map"
      final_per_iteration_table
    }

    //start to join the ${iteration} tables, null values are replaced with 0
    //this table is defined as a VAR because it will be clobbered when we add the array-type columns to it
    var outer_joined_table = {
      val joined_table_with_nulls = table_list.reduce { (a, b) =>
        a.join(b, Seq("Function_(Full)"), "outer")
      }
      val column_names = joined_table_with_nulls.columns

      joined_table_with_nulls.na.fill(0, column_names)
    }

    //create a ArrayType column (which contains all CPU time) and add to the table
    //the purpose of doing this is to make per-row calculation easier (e.g. calculate the average CPU_Time of each row)
    //Each column - let denote it 'x' - that we name in ${columns_of_interest} needs to be created an array that aggregates
    //col(x_1), col(x_2), ..., col(x_n) where n is equal to ${iteration}

    /*val table_with_array = {
      columns_eligible_to_create_an_array_for.foreach {
        case coi if (coi.contains("Function") == false) => {
          //${per_iteration_column_names} will look like Seq("A_1", "A_2", "A_3", ...)
          val per_iteration_column_names =
            (1 to iteration).map(idx => coi + s"_${idx}")
          val seq_of_columns: Seq[Column] =
            outer_joined_table.columns.foldLeft(Seq[Column]()) { (seq, cn) =>
              cn match {
                case cn if per_iteration_column_names.contains(cn) =>
                  seq :+ outer_joined_table.apply(cn)
                case _ => seq
              }
            }
          outer_joined_table = outer_joined_table.withColumn(
            coi + "_Array",
            array(seq_of_columns: _*)
          )
        }
        case _ => { /*do nothing*/ }
      }
      outer_joined_table
    }*/
    val table_with_array = generate_array_of_columns(outer_joined_table, columns_eligible_to_create_an_array_for)

    table_with_array.createOrReplaceTempView("table_with_array")
    //table_with_array.show()
    //table_with_array.printSchema()

    //generate the query plan to calculate UDFs (skewness, and average)
    var sql_query_plan = Array[String]()
    columns_eligible_to_create_an_array_for.foreach {
      case coi if (coi.contains("Function") == false) => {
        val array_name = coi + "_Array"

        sql_query_plan = sql_query_plan :+ ","
        sql_query_plan =
          sql_query_plan :+ ("skewness(`" + array_name + "`) AS `Skewness_of_" + coi + "`")

        sql_query_plan = sql_query_plan :+ ","
        sql_query_plan =
          sql_query_plan :+ ("avrg(`" + array_name + "`) AS `Average_of_" + coi + "`")
      }
      case _ => {}
    }
    //the sql query plan should now look like this: ", xxx, yyy, zzz, www, ..., aaa, bbb"
    //remove the first comma from the query plan
    sql_query_plan = sql_query_plan.tail

    //add the constant parts to the query plan
    sql_query_plan = "select *," +: sql_query_plan
    sql_query_plan = sql_query_plan :+ "from table_with_array"

    val sql_query_plan_text = sql_query_plan.mkString(" ")

    val calculated_table =
      spark.sql(
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
    val final_table = rearranged_table.columns.foldLeft(rearranged_table) {
      (tbl, column_name) =>
        val pattern_for_tiered_cpu_time = """(Idle|Poor|Ok|Ideal|Over)""".r
        column_name match {
          case cn if column_name.contains("Array") => tbl.drop(cn)
          case pattern_for_tiered_cpu_time.unanchored(x)
              if drop_tiered_cpu_time =>
            tbl.drop(column_name)
          case _ => tbl
        }
    }

    //write the table to file
    //repartition the table down to one partition so that only one file is generated

    if (save_result) {
      val result_file = new File(query_path, "result.csv")
      final_table
        .coalesce(1)
        .write
        .options(Map("sep" -> "|||", "header" -> "True"))
        .mode("overwrite")
        .csv(result_file.getPath())
      print(s"${query_path.getName()} is written to file\n")
    } else {
      final_table.printSchema()
      final_table.show()
    }
  }

  def generate_array_of_columns(
      df: DataFrame,
      col_list: Seq[String]
  ): DataFrame = {
    var result_table = df
    col_list.foreach {
      //coi is "column of interest"
      case coi if (coi.contains("Function") == false) => {
        //the coi's string might have some trailing numbers in the form of "(_\d+)*".E.g. "xxxxx_2_6_42". We need to pick columns that have the same substring before the "(_\d+)*" part.
        val coi_regex = (coi + """(_\d*)*""").r
        val seq_of_columns: Seq[Column] =
          result_table.columns.foldLeft(Seq[Column]()) { (seq, cn) =>
            cn match {
              case coi_regex(_) =>
                seq :+ result_table.apply(cn)
              case _ => seq
            }
          }
        result_table = result_table.withColumn(
          coi + "_Array",
          array(seq_of_columns: _*)
        )
      }
      case _ => { /*do nothing*/ }
    }
    result_table

  }



  // get_dataframe(query_path)(test_id, it_id) will be passed to Array.tabulate(n1: Int, n2: Int)(f: (Int, Int) ⇒ T)
  def get_dataframe(
      query_path: File,
      demand_wall_time: Boolean = false,
      drop_tiered_cpu_time: Boolean = false
  )(test_id_0: Int, iteration_id_0: Int): DataFrame = {
    val test_id = test_id_0 + 1
    val iteration_id = iteration_id_0 + 1

    if (demand_wall_time && !can_calculate_weighted_wall_time) {
      throw new Exception(
        "CLI demanded wall time approximation be calculated; but input data does not contain all necessary data."
      )
    }

    val csv_file = new File(query_path, s"test${test_id}/it${iteration_id}.csv")

    val raw_table = spark.read
      .options(
        Map(
          "sep" -> "|||",
          "header" -> "True",
          "inferSchema" -> "True",
          "enforceSchema" -> "False"
        )
      )
      .csv(csv_file.getPath())

    val original_table = raw_table
    //rename the columns so that no space character exists; all columns' name except the first column are added the suffix "_x" where x is ${idx}
    val original_columns = original_table.columns
    val renamed_columns = original_columns.map(rename_column_function(test_id, iteration_id))
    val renamed_table =
      (original_columns zip renamed_columns).foldLeft(original_table) {
        (tbl, pair) =>
          tbl.withColumnRenamed(pair._1, pair._2)
      }

    //only take record of function calls that exceed 0.05 CPU seconds
    //project the renamed table on only the columns we are interested in
    //val filter_criterion = s"CPU_Time_${idx}>0.05"
    val renamed_columns_of_interest =
      columns_of_interest.map(rename_column_function(test_id, iteration_id))
    val dummy_column = renamed_table("Function").as("Dummy")
    val filtered_table = renamed_table
      .withColumn("Dummy", dummy_column)
      //.filter(filter_criterion)
      .select("Dummy", renamed_columns_of_interest: _*)
      .drop("Dummy")

    //aggregate the table
    val agg_table = filtered_table.groupBy("Function_(Full)").sum()

    //rename the columns where a "sum" has been put before the metric name
    val sum_name_pattern = """sum\((.*)\)""".r
    val agg_table_renamed = agg_table.columns.foldLeft(agg_table) {
      (tbl, cn) =>
        cn match {
          case sum_name_pattern(pure_name) =>
            tbl.withColumnRenamed(cn, pure_name)
          case _ => tbl
        }
    }    

    val final_per_iteration_table = {
      if (can_calculate_weighted_wall_time && demand_wall_time) {
        val weighted_column =
          agg_table_renamed(s"CPU_Time:Effective_Time:Poor_${test_id}_${iteration_id}") / weights(
            "Poor"
          ) +
            agg_table_renamed(s"CPU_Time:Effective_Time:Ok_${test_id}_${iteration_id}") / weights(
              "Ok"
            ) +
            agg_table_renamed(
              s"CPU_Time:Effective_Time:Ideal_${test_id}_${iteration_id}"
            ) / weights(
              "Ideal"
            ) +
            agg_table_renamed(
              s"CPU_Time:Effective_Time:Over_${test_id}_${iteration_id}"
            ) / weights(
              "Over"
            )
        agg_table_renamed.withColumn(
          s"Wall_Time_Approx_${test_id}_${iteration_id}",
          weighted_column
        )
      } else agg_table_renamed
    }

    final_per_iteration_table
  }

  //the renaming function replaces all spaces with equal amount of underscores,
  //and adds a numerical suffix to the string if the string does not contain substring "Function"
  def rename_column_function(idx: Int)(str: String) = {
    str match {
      case x if str.contains("Function") => x.replace(" ", "_")
      case _                             => (str + s"_$idx").replace(" ", "_")
    }
  }

  def rename_column_function(idx: Int, jdx: Int)(str: String) = {
    str match {
      case x if str.contains("Function") => x.replace(" ", "_")
      case _                             => (str + s"_${idx}_${jdx}").replace(" ", "_")
    }
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
    required = false,
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
                |       └...""".stripMargin,
    argName = "reportLocation"
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
    short = 'n',
    default = Option(false),
    descr = "Default is false. Does not write to any file if set to true"
  )
  val include_list = opt[List[String]](
    name = "include_list",
    noshort = true,
    required = false,
    default = Option(List[String]()), //default is empty array
    descr =
      "Which queries to analyze. Will process all queries found under reportLocation if this option is skipped. E.g.: --include_list q1 q2 q6"
  )
  val demand_wall_time = opt[Boolean](
    name = "demand_wall_time",
    noshort = true,
    required = false,
    default = Option(false),
    descr =
      """Default is false. If set, the program will attempt to calculate the wall time from the four of the five tiers of CPU Time based on CPU utilization (Poor, Ok, Ideal, Over). 
              |However, if these source metrics are not listed in the columns_of_interest, the program will fail and exit.""".stripMargin
  )
  val drop_tiered_cpu_time = opt[Boolean](
    name = "drop_tiered_cpu_time",
    noshort = true,
    required = false,
    default = Option(false),
    descr =
      "Default is false. If set, will drop the five tiers of CPU time based on CPU utilization (Idle, Poor, Ok, Ideal, Over)."
  )
  val sort = opt[Boolean](
    name = "sort",
    required = false,
    default = Option(false),
    descr = "Default is false. If set columns will be sorted."
  )

  verify()
}
