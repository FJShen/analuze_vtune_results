#!/bin/bash

PROJECT_DIR="/home/shen449/intel/vtune/projects/rapids_tpch_pc01/"

CMD_PROTOTYPE="vtune -report hotspots -r r027hs -filter frame-domain=q1 -format csv -csv-delimiter \"|||\""

for idx in {1..5}
do
    FULL_CMD="$CMD_PROTOTYPE -filter frame=$idx > r027frame$idx.csv"
    echo $FULL_CMD
    eval $FULL_CMD
done