#!/bin/bash

python mr_wordcount.py ../book.txt -r hadoop \
       --output-dir word_count \
       --python-bin /opt/conda/default/bin/python
