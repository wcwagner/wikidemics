#!/home/william/.virtualenvs/quac/bin/python

import gzip
import datetime
import sys
import os
import glob
from collections import deque
import argparse
import sqlite3
import re
"""
USAGE: ./index_access_logs.py YYYY MM

Takes the pagecounts-raw csv files and stores them into SQLite db
"""


def process_pagecounts_file(file_dest, output_dir):
    # files are of form pagecounts-YYYYMMDD-HHMMSS.gz
    date_regex = "pagecounts-(\d{4})(\d{2})(\d{2})-(\d{2})"
    r = re.search(date_regex, file_dest)
    year = int(r.group(1))
    month = int(r.group(2))
    day = int(r.group(3))
    hour = int(r.group(4))
    # convert to datetime, in order to pad vals and keep a consistent format
    timestamp = datetime.datetime(year=year, month=month, day=day, hour=hour)
    y, m, d, h = timestamp.strftime("%Y-%m-%d-%H").split("-")

    #
    db_file = os.path.join(output_dir.format(year=y, month=m, day=d),
                           "{hour}.db".format(hour=h))

    if not os.path.exists(db_file):
        # make the directory to put the hourly pagecount.db file in
        try:
            os.makedirs(os.path.abspath(os.path.join(db_file, os.pardir)))
        except OSError:
            pass

    with gzip.open(file_dest, "rb") as pagecounts:
        # will hold the rows to insert into the SQL table
        previous_5_pages = deque(maxlen=5)
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
                print("Error proccessing line {0} "
                      "for file {1}".format(line, file_dest, file=sys.stderr))
                continue
            if (project, page_title) not in previous_5_pages:
                rows.append((project, page_title, num_accesses, content_size))
                # slide the 5 page window over
                previous_5_pages.append((project, page_title))
            # bulk insert, use cursor.executemany() -- which automatically
            # wraps inserts in BEGIN and COMMIT to greatly reduce overhead
            if len(rows) == 10000:
                pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Index Wikipedia Logs to db")
    parser.add_argument("year", type=str, help="Year to index in the db",
                        default=None)

    parser.add_argument("month", type=str, help="Month to index in the db",
                        default=None)

    args = parser.parse_args()

    if args:
        # get a list of the file paths for the pagecount csvs corresponding to
        # the year and month args -- SORTED
        path = "/media/william/Passport/AccessLogs/raw/{year}/{year}-{month}"\
               "/pagecounts*.gz".format(year=args.year, month=args.month)

        file_dests = sorted(glob.glob(path))
        output_dir = "/media/william/Passport/wikipeida/index/{year}/{month}/{day}"
        schema = "/media/william/Passport/wikipedia/pagecount_schema.sql"

        for dest in file_dests:
            process_pagecounts_file(dest, output_dir)




