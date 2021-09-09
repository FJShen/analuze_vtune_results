#!/bin/bash

SPARK_HOME=/opt/spark-3.1.2-bin-hadoop3.2/

bash ${SPARK_HOME}/bin/spark-submit --class AnalyzeDiff --packages org.rogach:scallop_2.12:4.0.4 \
/home/shen449/analyze_vtune_results/target/scala-2.12/analyzediff_2.12-1.0.jar \
-r /home/shen449/analyze_vtune_results/extracted_reports/2021_9_5 \
--demand_wall_time --drop_tiered_cpu_time \
--sort