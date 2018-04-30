# metadata
repo for handling metadata upload/processing/downloading

This is a collection of scripts that run to grab job files from AWS
and process them using a local HPC cluster. These scripts then upload
completed results to an S3 bucket and check periodically for the status of
each running job.

This repo runs two scripts:
- ```download_rawdata.py```: Downloads metadata job files first, then uses specific fields to download necessary fastq files. Then, copy job file and make executable.
- ```generate_jobs.py```: This script looks into a working directory and runs/checks/removes any jobs inside that directory.
    - If a job(jsonlike) is found but no jobdir present, script will submit to queue.
    - If a job is found and a jobdir is present:
        - if results are found, job was successful.
            - copy and upload log file (redundant but might be useful to have at toplevel)
            - copy results to local results dir
            - from one copy, remove files that won't be uploaded and upload that copy
            - remove job/log/results from working directory.
        - if no results found:
            - if job log is "done", job failed.
                - copy and upload log file
                - copy and upload job file
                - copy results directory to local results dir for debug
                - remove job/log/results from working directory
            - if job log is not "done", job is still running.
                - copy and upload current progress (log)