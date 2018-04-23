#!/bin/bash
#PBS -N john_doe_20180412.json
#PBS -o /home/bay001/projects/codebase/metadata/work_dir/john_doe_20180412.json.sh.out
#PBS -e /home/bay001/projects/codebase/metadata/work_dir/john_doe_20180412.json.sh.err
#PBS -V
#PBS -l walltime=72:00:00
#PBS -l nodes=1:ppn=1
#PBS -A yeo-group
#PBS -q home

# Go to the directory from which the script was called
cd $PBS_O_WORKDIR
module load dropseqtools;/home/bay001/projects/codebase/metadata/work_dir/john_doe_20180412.json

