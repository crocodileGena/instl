#!/bin/bash

python3.10 -m venv venv
source venv/bin/activate
python3.10 -m pip install --upgrade pip
python3.10 -m pip install -r requirements.txt
python3.10 -m pip install -r requirements_mac_only.txt

#conda create --name installEnv
#conda activate installEnv
#conda install -c conda-forge PyYaml -y