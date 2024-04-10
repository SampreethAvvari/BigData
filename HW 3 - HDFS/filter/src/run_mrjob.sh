#!/bin/bash


python mr_sql.py ../movies.csv -r hadoop \
       --output-dir movies_filter \
       --python-bin /opt/conda/default/bin/python
