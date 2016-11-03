import gzip
import datetime
import sys
import os
import glob
from collections import deque
import argparse
import sqlite3

argparser = argparse.ArgumentParser(description="Index Wikipedia Logs to database")
argparser.add_argument("year", type=str, help="Year to index in the database", default=None)

argparser.add_argument("month", type=str, help="Month to index in the database", default=None)

args = argparser.parse_args()

def process_pagecounts_file(file_dest):

    with gzip.open(file_dest, "rb") as pagecounts:
        # will hold the rows to insert into the SQL table
        rows = list()
        for line in pagecounts:

            line_fields = line.split()
            # first column is project name (e.g. fr.b --> france's wikibooks)
            project = line_fields[0]
            # title of the page accessed (can have spaces in it)
            page_title = b" ".join(line_fields[1:-2])
            try:
                # number of accesses for that month
                num_accesses = int(line_fields[-2])

                # size of response bytes for all accesses
                content_size = int(line_fields[-1])
            except:
                continue

            rows.append((project, page_title, num_accesses, content_size))



