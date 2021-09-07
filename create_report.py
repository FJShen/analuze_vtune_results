import re #pattern matching
from pathlib import Path 
import glob
import argparse
import subprocess
import os

def main():
    project_directory="/home/shen449/intel/vtune/projects/rapids_tpch_pc01/"
    matching_time_pattern="2021_8_19"
    dry_run = True
    default_target_dir = Path("./extracted_reports/" , matching_time_pattern )
    include_list = []
    exclude_list = []

    #get all directories that match the matching_time_pattern
    p = Path(project_directory)
    dir_list = [x for x in p.iterdir() if x.is_dir()]
    dir_list = [x for x in dir_list if len(re.findall(matching_time_pattern, x.name))>0]
    pass

    target_dir = default_target_dir

    #create vtune reports for these directories
    for x in dir_list:
        # x looks like this: "2021_9_5_22_44_TPCH_Q2_IT6_TEST1_1thrd_gc_noXcomp_fastdebugJVM"
        query_name = re.findall("(?<=_Q)\d+(?=\D)", x.name)        
        query_name = "q" + str(query_name[0])
        
        if(len(include_list) > 0 and (query_name not in include_list)):
            continue
        
        if(len(exclude_list) > 0 and (query_name in exclude_list)):
            continue

        test_id = re.findall("(?<=_TEST)\d+(?=\D)", x.name)
        test_id = int(test_id[0])
        
        iteration = re.findall("(?<=_IT)\d+(?=\D)", x.name)
        iteration = int(iteration[0])
    
        per_query_result_dir = target_dir / Path(query_name) / Path("test" + str(test_id)) #e.g. "<target_dir>/q1/test2/"
        
        os.makedirs(per_query_result_dir.as_posix(), exist_ok=True)
        
        for frame_id in range(1, iteration+1):
            #this for-loop will create the following csv files:  <target_dir>/q1/test2/it1.csv, <target_dir>/q1/test2/it2.csv, etc.
            vtune_data_dir = x.as_posix() 
            
            cmd = ["vtune"]
            cmd.extend(["-report", "hotspots"])
            cmd.extend(["-r", vtune_data_dir])
            cmd.extend(["-filter", "frame-domain="+query_name])
            cmd.extend(["-filter", "frame="+str(frame_id)])
            cmd.extend(["-filter", "module=[Compiled Java code]"])
            cmd.extend(["-filter", "module=[Dynamic code]"])
            cmd.extend(["-format", "csv"])
            cmd.extend(["-csv-delimiter", "|||"])
            
            output_file = per_query_result_dir / Path("it"+str(frame_id)+".csv")
            
            final_cmd = " ".join(cmd)
            print(final_cmd)
            if(not dry_run):
                with open(output_file.as_posix(), 'w') as ofile:
                    subprocess.run(cmd, stdout=ofile)

                    
        

if __name__=="__main__":
    main()