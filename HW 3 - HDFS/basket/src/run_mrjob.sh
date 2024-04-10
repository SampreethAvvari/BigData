#!/bin/bash

python mr_basket.py ../data/*.csv -r hadoop \
       --output-dir basket_output \
       --python-bin /opt/conda/default/bin/python

