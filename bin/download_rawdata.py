#!/usr/bin/env python

"""
parses json-like file and creates a download script.
"""
import argparse
import json
import os
import glob
import subprocess
import shutil
import boto3
import botocore


def download_metadata_from_s3(bucket='metadata-pipelines', output_dir="../json_dir"):
    """
    Downloads metadata files (json) to local directory (json_dir) from
    a bucket (metadata-pipelines)

    :param bucket: basestring
        bucket name (minus the s3:// prefix)
    :param output_dir:
    :return:
    """
    s3client = boto3.client('s3')

    # if s3 prefix exists, remove it.
    if bucket.startswith("s3://"):
        bucket = bucket[5:]

    try:
        for obj in s3client.list_objects(Bucket=bucket)['Contents']:
            if obj['Key'].endswith('.json'):
                local_file_name = os.path.join(output_dir, obj['Key'])
                print("{} found in bucket {}".format(obj['Key'], bucket))
                s3client.download_file(bucket, obj['Key'], local_file_name)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


def download_rawdata_from_s3(fn, bucket='metadata-pipelines', output_dir="../work_dir"):
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
        s3.Bucket(bucket).download_file(fn, out_file)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


def download_raw_files_from_json(fn, output_dir, bucket):
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
                    print("downloading {}".format(sample['read1']['path']))
                    download_rawdata_from_s3(
                        fn=sample['read1']['path'],
                        bucket=bucket,
                        output_dir=output_dir
                    )
                else:
                    print("R1 file ({}) found in {}".format(sample['read1']['path'], output_dir))
                    # TODO: check validity of existing file? Checksum?
                    pass
                if not os.path.exists(os.path.join(output_dir, sample['read2']['path'])):
                    print("downloading {}".format(sample['read2']['path']))
                    download_rawdata_from_s3(
                        fn=sample['read2']['path'],
                        bucket=bucket,
                        output_dir=output_dir
                    )
                else:
                    print("R2 file ({}) found in {}".format(sample['read2']['path'], output_dir))
                    # TODO: check validity of existing file? Checksum?
                    pass

        except ValueError:
            print("Couldn't load the json-like file!")
    return 0


def move_jobfile(src, dest):
    """
    Moves the job file from one directory to another, and
    makes it executible.

    :param src: basestring
        file path
    :param dest: basestring
        destination file path
    :return:
    """
    shutil.copyfile(src, dest)
    subprocess.check_call("chmod +x {}".format(dest), shell=True)


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
        default="../json_dir"
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
        default="../work_dir"
    )

    args = parser.parse_args()
    # jsonlike = args.jsonlike_file
    json_dir = args.json_dir
    work_dir = args.work_dir
    bucket = args.bucket

    download_metadata_from_s3(bucket=bucket, output_dir=json_dir)

    json_files = glob.glob(os.path.join(json_dir, '*.json'))
    print("json files found in {}: {}".format(json_dir, json_files))

    for json_file in json_files:
        print("downloading and moving: {}".format(json_file))
        download_raw_files_from_json(
            json_file, work_dir, bucket
        )
        move_jobfile(json_file, os.path.join(work_dir, os.path.basename(json_file)))

if __name__ == "__main__":
    main()