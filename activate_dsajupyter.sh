#!/bin/bash
##ipcluster start --n=6 need to make this a supervisor process
#source  /home/dgutman/anaconda/envs/dsa_girder/bin/activate
source activate dsa_girder
jupyter  notebook  --config=/home/dgutman/.jupyter/jupyter_notebook_config.py --notebook-dir=/home/dgutman/devel/CDSA_LoadTools/ --port 8899
