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
import logging
import time
import sys
import re

__version__ = '0.0.1'


def make_tarfile(output_filename, source_dir, logger):
    """
    Generates tar.gz file from source_dir.

    :param output_filename: basestring
        typically ${SRC}.tar.gz
    :param source_dir: basestring
        the directory to be tarballed
    :param logger: logging.Logger()
        Logger object
    :return:
    """
    logger.info("Making tar file (may take awhile..)")
    if not os.path.exists(output_filename):
        logger.info("{} doesn't exist, generating tar file.".format(output_filename))
        with tarfile.open(output_filename, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))
    else:
        logger.info("{} exists, will not overwrite.".format(output_filename))
    logger.info("Done writing tar file ({})".format(output_filename))
    logger.info("Sleeping for 15s.")
    time.sleep(15)


def status(fn, logger):
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
    logger.info("Checking run dir: {}".format(run_dir))
    logger.info("Checking results dir: {}".format(results_dir))

    if os.path.exists(run_dir):
        log_file = glob.glob(os.path.join(run_dir, "*_LOG.txt"))
        logger.info("Checking log: {}".format(log_file))
        try:
            assert len(log_file) == 1
            log_file = log_file[0]

            if os.listdir(results_dir) == []:  # no results yet TODO: get number of final outputs and check.
                if done(log_file):
                    return 1, run_dir, log_file
                else:
                    return 0, run_dir, log_file
            else:  # we have results
                return 0, run_dir, log_file
        except AssertionError:
            if len(log_file) == 0:
                logger.warning("No log file found in: {}".format(run_dir))
                return 0, run_dir, None
            elif len(log_file) > 1:
                logger.warning("More than one log file found in: {}".format(run_dir))
                raise
    else:
        logger.info("Run directory doesn't exist for: {}".format(fn))
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
            if line.rstrip().endswith("Final process status is success"):  # cwltool final output
                return True
            elif line.rstrip().endswith("Joining real-time logging server thread."):  # toil final output
                return True
            elif line.rstrip().endswith("KeyboardInterrupt"):  # keyboard interrupt
                return True
            elif "permanentFail" in line.rstrip():  # this doesn't really occur at the end but entire job will fail if permanentFail.
                return True
    return False


def copy_files(src, dest, logger):
    """
    Copies a file locally.

    :param src: basestring
        source file
    :param dest: basestring
        destination directory name or file name
    :param logger: logging.Logger()
        Logger object.
    :return:
    """
    try:
        logger.info("Copying {} to {}".format(src, dest))
        shutil.copy(src, dest)
        logger.info("Done copying {} to {}".format(src, dest))
    except Exception as e:
        logger.error(e)
        raise


def copy_dir(src, dest, logger):
    """
    Copies a file or directory locally.
    ie. copying path/to/src/ to otherpath/to/dest/ -> otherpath/to/dest/src

    :param src: basestring
    :param dest: basestring
        local destination
    :param logger: logging.Logger
        Logger object
    :return:
    """

    if not src.endswith('/'):
        src = src + '/'

    copy_directory = os.path.join(
        dest, os.path.dirname(src).split('/')[-1] + "/"
    )

    try:
        logger.info("Copying {} to {}.".format(src, copy_directory))
        shutil.copytree(src, copy_directory)
        logger.info("Done copying {} to {}".format(src, copy_directory))
    except OSError as exc:  # python >2.5
        if exc.errno == errno.ENOTDIR:
            logger.warning("Warning, {} not a directory.".format(src))
            shutil.copy(src, copy_directory)
        elif exc.errno == errno.EEXIST:
            logger.warning("Warning, folder {} exists, will not overwrite.".format(src))
        else:
            logger.error(exc)
            raise
    logger.info("Sleeping for 15s.")
    time.sleep(15)


def copy_files_aws(src, dest, logger):
    """
    upload processed data (typically res dir) to s3

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
        logger.info("Done uploading {} to aws ({})".format(src, dest))
        logger.info("Sleeping for 15s.")
        time.sleep(15)
    except Exception as e:
        logger.error(e)
        raise


def copy_dir_aws(src, bucket, logger):
    """
    Copies a directory from local to AWS

    :param src: basestring
        local source filename
    :param bucket: basestring
        aws s3 bucket name
    :param logger: logging.Logger()
        Logger object
    :return:
    """
    if not bucket.startswith('s3://'):
        bucket = 's3://' + bucket

    if not src.endswith('/'):
        src = src + '/'

    upload_dir = os.path.dirname(src).split('/')[-1] + "/"

    cmd = 'aws s3 cp {} {} --recursive'.format(
        src,
        os.path.join(bucket, upload_dir)
    )
    try:
        logger.info("Uploading to aws: {}".format(cmd))
        ret = subprocess.check_call(cmd, shell=True)
        logger.info("Done uploading {} to aws ({}) with a return code of: {}".format(src, bucket, ret))
        logger.info("Sleeping for 15s.")
        time.sleep(15)
    except Exception as e:
        logger.error(e)


def remove(to_remove, logger):
    """
    Removes a directory or file using shutil or os.

    :param to_remove: basestring
        the directory/file to remove
    :return:
    """
    try:
        logger.info("Removing {}".format(to_remove))
        shutil.rmtree(to_remove)
    except OSError:
        logger.info("Removing {}".format(to_remove))
        os.remove(to_remove)


def remove_file(to_remove, logger):
    """
    Remove a single file using os.remove().

    :param to_remove: basestring
    :param logger:
    :return:
    """

    logger.info("Removing {}".format(to_remove))
    if os.path.exists(to_remove):
        os.remove(to_remove)
    else:
        logger.warning("Warning, {} does not exist (and so will not remove)".format(to_remove))
    time.sleep(1)


def remove_unnecessary_intermediates(res, logger, pipeline='dropseqtools'):
    """
    Remove intermediate files as this dramatically reduces upload of res
    folder.

    :param res: basestring
        results directory
    :param logger: logging.Logger
        Logger object
    :param pipeline: basestring
        Because different pipelines produce differently formatted results,
        this param provides a switch among files to remove for each pipeline.
    :return:
    """
    logger.info("Removing unnecessary intermediate files.")
    if pipeline == 'dropseqtools':
        suffixes_to_remove = [
            ".sam$",
            ".tagged([\d]+-[\d]+).bam$",
            '.tagged([\d]+-[\d]+).tagged([\d]+-[\d]+).bam$',
            '.tagged([\d]+-[\d]+).tagged([\d]+-[\d]+).filtered.trimmed_smart.bam$',
            '.tagged([\d]+-[\d]+).tagged([\d]+-[\d]+).filtered.trimmed_smart.polyA_filtered.bam$',
            '.tagged([\d]+-[\d]+).tagged([\d]+-[\d]+).filtered.trimmed_smart.polyA_filtered.STARAligned.out.bam$',
            '.tagged([\d]+-[\d]+).tagged([\d]+-[\d]+).filtered.trimmed_smart.polyA_filtered.STARUnmapped.out.mate1$',
            '.tagged([\d]+-[\d]+).tagged([\d]+-[\d]+).filtered.trimmed_smart.polyA_filtered.STARAligned.out.namesorted.bam$',
            '.tagged([\d]+-[\d]+).tagged([\d]+-[\d]+).filtered.trimmed_smart.polyA_filtered.STARAligned.out.namesorted.merged.bam$',
            '.tagged([\d]+-[\d]+).tagged([\d]+-[\d]+).filtered.trimmed_smart.polyA_filtered.STARAligned.out.namesorted.merged.TaggedGeneExon.bam$',
        ]
    else:
        suffixes_to_remove = []  # TODO: cellranger
    for suffix in suffixes_to_remove:
        for f in os.listdir(os.path.join(res, "results")):
            regex = '[\w\d]+' + suffix
            match = re.compile(regex).match
            if match(f):
                remove_file(to_remove=os.path.join(res, "results", f), logger=logger)
    logger.info("Done removing unnecessary files.")


def submit_job(fn, work_dir, logger, pipeline):
    """
    Creates a qsub job node and submits jobs to queue.

    :param fn: basestring
        filename of json/yaml executable
    :param work_dir: basestring
        working directory where the qsub script should go.
    :return:
    """
    os.chdir(work_dir)
    jobname = os.path.splitext(os.path.basename(fn))[0]
    bash_script = os.path.join(work_dir, os.path.basename(fn) + ".sh")
    if not os.path.exists(bash_script):  # bash script hasn't been generated
        logger.info("Creating submission script {}.".format(bash_script))
        priming_call = "source ~/.bashrc\nmodule load {}\ncd {}\n./{}".format(pipeline, work_dir, os.path.basename(fn))  # TODO: remove hardcoded dropseqtools module load
        Submitter(
            [priming_call],
            jobname,
            sh=bash_script,
            array=False,
            nodes=1,
            ppn=8,
            walltime='72:00:00',
            submit=True
        )
        logger.info("Sleeping for 15s.")
        time.sleep(15)
    else:
        logger.info("Script {} submitted, will not recreate.".format(bash_script))


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
        "--log_dir",
        required=False,
        type=str,
        default="/home/bay001/projects/codebase/metadata/logs"
    )
    parser.add_argument(
        "--pipeline",
        required=False,
        type=str,
        default="dropseqtools"
    )
    parser.add_argument(
        "--bucket",
        required=False,
        type=str,
        default="metadata-results"
    )
    # Process args
    args = parser.parse_args()
    # jsonlike_files = args.jsonlike_files
    bucket = args.bucket
    work_dir = args.work_dir
    results_dir = args.results_dir
    log_dir = args.log_dir
    pipeline = args.pipeline
    jsonlike_files = glob.glob(os.path.join(work_dir, '*.json'))

    # Process logs
    logger = logging.getLogger('status')
    logger.setLevel(logging.INFO)

    ih = logging.FileHandler(os.path.join(log_dir, 'status.txt'))
    ih.setLevel(logging.INFO)
    logger.addHandler(ih)

    eh = logging.FileHandler(os.path.join(log_dir, 'status.err'))
    eh.setLevel(logging.ERROR)
    logger.addHandler(eh)

    wh = logging.FileHandler(os.path.join(log_dir, 'status.txt'))
    wh.setLevel(logging.WARNING)
    logger.addHandler(wh)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ih.setFormatter(formatter)
    eh.setFormatter(formatter)
    logger.info("starting program")


    for jsonlike in jsonlike_files:
        logger.info("json file: {}".format(jsonlike))
        error, res, log_file = status(jsonlike, logger)
        if error == 1:  # job failed
            logger.info("Job {} failed. ({})".format(jsonlike, res))
            logger.error("Job {} failed. ({})".format(jsonlike, res))
            # tarred = res + ".tar.gz"

            # make_tarfile(output_filename=tarred, source_dir=res, logger=logger)
            copy_files_aws(src=jsonlike, dest=bucket, logger=logger)
            # copy_files_aws(src=tarred, dest=bucket, logger=logger)
            copy_dir(src=res, dest=results_dir, logger=logger)  # make backup copy on TSCC

            logger.info("cleaning up finished job {}".format(jsonlike))
            remove(res, logger)
            # remove(tarred, logger)
            remove_file(jsonlike, logger)
        elif error == 0:
            if res == None:  # job hasn't run yet
                logger.info("Job {} hasn't run yet ({})".format(jsonlike, res))
                submit_job(
                    fn=jsonlike,
                    work_dir=work_dir,
                    logger=logger,
                    pipeline=pipeline
                )
            elif log_file == None:
                logger.info("Job {} submitted but hasn't started yet (in queue)".format(jsonlike))
            else:
                if(os.listdir(os.path.join(res, 'results')) != []):  # job is finished
                    logger.info("Job {} finished ({})".format(jsonlike, res))
                    # tarred = res + ".tar.gz"

                    # make_tarfile(output_filename=tarred, source_dir=res, logger=logger)
                    copy_files_aws(src=jsonlike, dest=bucket, logger=logger)
                    # copy_files_aws(src=tarred, dest=bucket, logger=logger)
                    copy_dir(src=res, dest=results_dir, logger=logger)  # make backup copy on TSCC
                    remove_unnecessary_intermediates(
                        res=res, logger=logger, pipeline=pipeline
                    )
                    copy_dir_aws(src=res, bucket=bucket, logger=logger)

                    logger.info("cleaning up finished job {}".format(jsonlike))
                    remove(res, logger)
                    # remove(tarred, logger)
                    remove_file(jsonlike, logger)
                else:  # job still running
                    logger.info("Job {} still running, copying {}".format(jsonlike, res))
                    copy_files_aws(src=log_file, dest=bucket, logger=logger)


if __name__ == "__main__":
    main()
