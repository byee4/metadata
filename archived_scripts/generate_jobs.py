#!/usr/bin/env python

"""
This script looks into a working directory and
runs/checks/removes any jobs inside that directory.
If a job(jsonlike) is found but no jobdir present, script will submit to queue.
If a job is found and a jobdir is present:
  - if results are found, job was successful.
      - copy and upload log file (redundant but might be useful to have at toplevel)
      - tar, copy and upload results
      - tar and copy results to local results dir
      - remove job/log/results from working directory.
  - if no results found:
    - if job log is "done", job failed.
      - copy and upload log file
      - copy and upload job file
      - tar and copy results directory to local results dir for debug
      - remove job/log/results from working directory
    - if job log is not "done", job is still running.
      - copy and upload current progress (log)
"""
import argparse
import os
import glob
import shutil
import errno
import subprocess
from qtools import Submitter
import tarfile
import boto3
import botocore


def make_tarfile(output_filename, source_dir):
    """
    Generates tar file from source_dir.
    """
    if not os.path.exists(output_filename):
        with tarfile.open(output_filename, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))
    else:
        print("{} exists, will not overwrite.".format(output_filename))


def status(fn):
    """
    Returns 1, log_file if job failed
    Returns 0, results dir if job succeed
    Returns 0, log_file if job still running
    Returns 0, None if no job exists in working directory

    :param fn: basestring
        json-like filename
    :return to_copy:
        file or directory to copy
    """
    run_dir = os.path.splitext(fn)[0]
    results_dir = os.path.join(run_dir, 'results')
    if os.path.exists(run_dir):
        log_file = glob.glob(os.path.join(run_dir, "*_LOG.txt"))
        print("Log files found: {}".format(log_file))
        assert len(log_file) == 1
        log_file = log_file[0]

        if os.listdir(results_dir) == []:  # no results yet

            if done(log_file):
                return 1, run_dir, log_file
            else:
                return 0, run_dir, log_file
        else:  # we have results
            return 0, run_dir, log_file
    else:
        print("job not running")
        return 0, None, None


def done(log_file):
    """
    Determines whether or not a run is still going given a TOIL log file
    :param log_file: basestring
        path of TOIL log file
    :return:
    """
    with open(log_file, 'r') as f:
        for line in f:
            if line.rstrip().endswith("Joining real-time logging server thread."):
                return True
            elif line.rstrip().endswith("KeyboardInterrupt"):
                return True
            elif "permanentFail" in line.rstrip():  # this doesn't really occur at the end but entire job will fail if permanentFail.
                return True
    return False


def copy_files(src, dst):
    """ Copies a file """
    try:
        shutil.copy(src, dst)
    except Exception as e:
        print(e)
        return 1


def copy_dir(src, dst):
    """ Copies a file or directory """
    try:
        shutil.copytree(src, dst)
    except OSError as exc:  # python >2.5
        if exc.errno == errno.ENOTDIR:
            shutil.copy(src, dst)
        else:
            raise


def copy_files_aws(src, dest='metadata-results'):
    """
    Download raw data (typically fastq) from s3

    :param fn: basestring
        filename of the object that needs to be downloaded.
    :param bucket: basestring
        bucket name (minus the s3:// prefix)
    :param output_dir: basestring
        output directory where the rawdata should go.
    :return:
    """
    s3 = boto3.client('s3')

    try:
        s3.upload_file(
            src, dest, os.path.basename(src), ExtraArgs={'ACL': 'public-read'}
        )
        print("Done copying {}".format(src))
    except Exception as e:
        print(e)
        raise


def remove(to_remove):
    """
    Removes a directory or file using shutil or os.

    :param to_remove: basestring
        the directory/file to remove
    :return:
    """
    try:
        shutil.rmtree(to_remove)
    except OSError:
        os.remove(to_remove)


def submit_job(fn, work_dir):
    """
    Creates a qsub job node and submits jobs to queue.

    :param fn: basestring
        filename of json/yaml executable
    :param work_dir: basestring
        working directory where the qsub script should go.
    :return:
    """
    os.chdir(work_dir)
    jobname = os.path.basename(fn)
    bash_script = os.path.join(work_dir, os.path.basename(fn) + ".sh")
    if not os.path.exists(bash_script):  # bash script hasn't been generated
        print("creating submission script.")
        priming_call = "module load dropseqtools;{}".format(fn)  # TODO: remove hardcoded dropseqtools module load
        Submitter(
            [priming_call],
            jobname,
            sh=bash_script,
            array=False,
            nodes=1,
            ppn=1,
            walltime='72:00:00',
            submit=True
        )
    else:
        print("script submitted, will not recreate.")

def main():
    """
    Main program.
    """
    parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     "--jsonlike_files", "--jsonlike",
    #     required=True,
    #     type=str,
    #     nargs='+',
    #     default=None
    # )
    parser.add_argument(
        "--work_dir",
        required=False,
        type=str,
        default="/home/bay001/projects/codebase/metadata/work_dir"
    )
    parser.add_argument(
        "--results_dir",
        required=False,
        type=str,
        default="/home/bay001/projects/codebase/metadata/results_dir"
    )
    parser.add_argument(
        "--bucket",
        required=False,
        type=str,
        default="metadata-results"
    )
    args = parser.parse_args()
    # jsonlike_files = args.jsonlike_files
    bucket = args.bucket
    work_dir = args.work_dir
    results_dir = args.results_dir
    jsonlike_files = glob.glob(os.path.join(work_dir, '*.json'))

    for jsonlike in jsonlike_files:
        print("json file: {}".format(jsonlike))
        error, res, log_file = status(jsonlike)
        if error == 1:  # job failed
            print("res: {}, error: {}, log: {}".format(res, error, log_file))
            print("Job {} failed, ({})".format(jsonlike, res))
            tarred = res + ".tar.gz"
            print("Making tar file (may take awhile..)")
            make_tarfile(tarred, res)
            copy_files_aws(src=log_file, dest=bucket)
            copy_files_aws(src=jsonlike, dest=bucket)
            copy_files(tarred, results_dir)  # make backup copy on TSCC
            print("cleaning up failed job {}".format(jsonlike))
            remove(tarred)
            remove(res)
            remove(jsonlike)
        elif error == 0:
            if res == None:  # job hasn't run yet
                print("Job {} not run, need to submit.".format(jsonlike))
                submit_job(jsonlike, work_dir)
            else:
                if(os.listdir(os.path.join(res, 'results')) != []):  # job is finished
                    print(os.listdir(res))
                    print("Job {} finished ({})".format(jsonlike, res))
                    tarred = res + ".tar.gz"
                    print("Making tar file (may take awhile..)")
                    make_tarfile(tarred, res)
                    copy_files_aws(src=jsonlike, dest=bucket)
                    copy_files_aws(src=tarred, dest=bucket)
                    copy_files(tarred, results_dir)  # make backup copy on TSCC
                    print("cleaning up finished job {}".format(jsonlike))
                    remove(res)
                    remove(tarred)
                    remove(jsonlike)
                else:  # job still running
                    print("Job {} still running, copying {}".format(jsonlike, res))
                    copy_files_aws(src=log_file, dest=bucket)

if __name__ == "__main__":
    main()
