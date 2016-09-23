#!/bin/bash
##ipcluster start --n=6 need to make this a supervisor process
#source  /home/dgutman/anaconda/envs/dsa_girder/bin/activate
source /home/dgutman/anaconda/envs/dsa_girder/bin/activate dsa_girder
/home/dgutman/anaconda/envs/dsa_girder/bin/jupyter  notebook  --config=/home/dgutman/.jupyter/jupyter_notebook_config.py --notebook-dir=/home/dgutman/devel/CDSA_LoadTools/
