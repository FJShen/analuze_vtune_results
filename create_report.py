import re #pattern matching
from pathlib import Path 
import glob
import argparse

def main():
    project_directory="./"
    matching_time_pattern="2021_8_19"

    #get all directories
    p = Path(project_directory)
    dir_list = [x for x in p.iterdir() if x.is_dir()]
    dir_list = [x for x in dir_list if len(re.findall(matching_time_pattern, x.name))>0]
    pass

if __name__=="__main__":
    main()