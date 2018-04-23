#!/usr/bin/env bash

conda remove -y -n metadata --all
conda create -y -n metadata python=3.6
source activate metadata

conda install -y -c conda-forge awscli
pip install boto3

