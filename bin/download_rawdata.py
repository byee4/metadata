#!/usr/bin/env python

"""
This script will download files from an amazon s3 box.
First, it will look for any files (json) and download to a
json_folder to store all jobs submitted.
Then, it will download any raw files specified in the document
and create an executable copy of each jobfile in a work_folder.

"""
import argparse
import json
import os
import glob
import subprocess
import shutil
import boto3
import botocore
import logging


def download_metadata_from_s3(output_dir, bucket, ext, logger):
    """
    Downloads metadata files (json) to local directory (json_dir) from
    a bucket (metadata-pipelines)

    :param bucket: basestring
        bucket name (minus the s3:// prefix)
    :param ext: basestring
        name of file extension (should only be json or in some cases yaml)
    :param output_dir: basestring
        name of output directory (local) to place aws files into.
    :return:
    """
    s3client = boto3.client('s3')

    # if s3 prefix exists, remove it.
    if bucket.startswith("s3://"):
        bucket = bucket[5:]

    try:
        for obj in s3client.list_objects(Bucket=bucket)['Contents']:
            if obj['Key'].endswith(ext):
                local_file_name = os.path.join(output_dir, obj['Key'])
                logger.info("{} found in bucket {}".format(obj['Key'], bucket))
                s3client.download_file(bucket, obj['Key'], local_file_name)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error("Error downloading {} files from {}".format(bucket, ext))
        else:
            raise


def download_rawdata_from_s3(fn, bucket, output_dir, logger):
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
    s3 = boto3.resource('s3')
    out_file = os.path.join(output_dir, os.path.basename(fn))

    try:
        logger.info("{} found in bucket {}".format(fn, bucket))
        s3.Bucket(bucket).download_file(fn, out_file)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error("The object {} does not exist.".format(fn))
        else:
            raise


def download_raw_files_from_json(fn, output_dir, bucket, logger):
    """
    Takes a jsonlike (json file plus a /usr/bin/env header) and
    writes to an output file the path to each specified read.
    This function is specific to the json/YAML structure described
    in the dropseqtools pipeline definition spec.

    :param fn: basestring
        json-like filename
    :param bucket: basestring
        by default, writes a script that pulls from metadata-pipelines
    :return success: int
        returns 0
    """
    with open(fn) as f:
        f.readline()  # skips the /usr/bin/env line
        try:
            data = json.load(f)
            for sample in data['samples']:
                if not os.path.exists(os.path.join(output_dir, sample['read1']['path'])):
                    logger.info("Downloading {}".format(sample['read1']['path']))
                    download_rawdata_from_s3(
                        fn=sample['read1']['path'],
                        bucket=bucket,
                        output_dir=output_dir,
                        logger=logger
                    )
                else:
                    logger.info("R1 file ({}) found in {}".format(sample['read1']['path'], output_dir))
                    # TODO: check validity of existing file? Checksum?
                    pass
                if not os.path.exists(os.path.join(output_dir, sample['read2']['path'])):
                    logger.info("Downloading {}".format(sample['read2']['path']))
                    download_rawdata_from_s3(
                        fn=sample['read2']['path'],
                        bucket=bucket,
                        output_dir=output_dir,
                        logger=logger
                    )
                else:
                    logger.info("R2 file ({}) found in {}".format(sample['read2']['path'], output_dir))
                    # TODO: check validity of existing file? Checksum?
                    pass

        except ValueError:
            logger.error("Couldn't load {}".format(fn))
    return 0


def move_and_make_exec(src, dest, logger):
    """
    Moves the job file from one directory to another, and
    makes it executible.

    :param src: basestring
        file path
    :param dest: basestring
        destination file path
    :return:
    """
    try:
        logger.info("Copying file from {} to {}".format(src, dest))
        shutil.copyfile(src, dest)
    except Exception as e:
        logger.error(e)
        raise
    try:
        logger.info("Making {} executable.".format(dest))
        subprocess.check_call("chmod +x {}".format(dest), shell=True)
    except Exception as e:
        logger.error(e)


def main():
    """
    Main program.
    """
    parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     "--jsonlike_file", "--jsonlike",
    #     required=True,
    #     type=str,
    #     default=None
    # )
    parser.add_argument(
        "--json_dir",
        required=False,
        type=str,
        default="/projects/ps-yeolab3/bay001/codebase/metadata/json_dir"
    )
    parser.add_argument(
        "--bucket",
        required=False,
        type=str,
        default="metadata-pipelines"
    )
    parser.add_argument(
        "--work_dir",
        required=False,
        type=str,
        default="/projects/ps-yeolab3/bay001/codebase/metadata/work_dir"
    )
    parser.add_argument(
        "--results_dir",
        required=False,
        type=str,
        default="/projects/ps-yeolab3/bay001/codebase/metadata/results_dir"
    )
    parser.add_argument(
        "--log_dir",
        required=False,
        type=str,
        default="/projects/ps-yeolab3/bay001/codebase/metadata/logs"
    )
    parser.add_argument(
        "--ext",
        required=False,
        type=str,
        default=".json",
        help="job files from bucket have this extension (.json default)"
    )

    args = parser.parse_args()
    # jsonlike = args.jsonlike_file
    json_dir = args.json_dir
    work_dir = args.work_dir
    results_dir = args.results_dir
    log_dir = args.log_dir
    bucket = args.bucket
    ext = args.ext

    # Process logs
    logger = logging.getLogger('status')
    logger.setLevel(logging.INFO)

    ih = logging.FileHandler(os.path.join(log_dir, 'status.txt'))
    ih.setLevel(logging.INFO)
    logger.addHandler(ih)

    eh = logging.FileHandler(os.path.join(log_dir, 'status.err'))
    eh.setLevel(logging.ERROR)
    logger.addHandler(eh)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ih.setFormatter(formatter)
    eh.setFormatter(formatter)
    logger.info("Starting download program")

    # download json files from an amazon s3 bucket
    download_metadata_from_s3(output_dir=json_dir, bucket=bucket, ext=ext, logger=logger)

    json_files = glob.glob(os.path.join(json_dir, '*.json'))
    logger.info("{} JSON files found in {}".format(len(json_files), json_dir))
    for json_file in json_files:
        logger.info("Processing file: {}".format(json_file))
        download_raw_files_from_json(
            fn=json_file,
            output_dir=work_dir,
            bucket=bucket,
            logger=logger
        )
        res = os.path.splitext(os.path.basename(json_file))[0]
        if os.path.exists(os.path.join(results_dir, res)):
            logger.info(
                "Results folder found ({}), will not reprocess (move to work dir).".format(
                    res
                )
            )
        else:
            logger.info(
                "Results not generated yet, moving {} to work dir for processing.".format(
                    json_file
                )
            )
            move_and_make_exec(
                src=json_file,
                dest=os.path.join(work_dir, os.path.basename(json_file)),
                logger=logger
            )

if __name__ == "__main__":
    main()
