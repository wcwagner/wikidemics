from urllib.parse import unquote_plus
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from datetime import date, timedelta
import re
# matches the pagecount-raw entires,
# group1: pagetitle | group2: pageviews | group3: responsebytes
en_wiki_regex = re.compile("en (\S+) ([0-9]+) ([0-9]+)")

# Excludes pages outside of namespace 0 (ns0)
exclude_titles_regex = re.compile('(Media|Special' +
'|Talk|User|User_talk|Project|Project_talk|File' +
'|File_talk|MediaWiki|MediaWiki_talk|Template' +
'|Template_talk|Help|Help_talk|Category' +
'|Category_talk|Portal|Wikipedia|Wikipedia_talk)\:(.*)')

date_regex = re.compile("pagecounts-(\d{8})")

def is_valid_title(title):
    is_outside_namespace_zero = namespace_titles_regex.match(title)
    if is_outside_namespace_zero is not None:
        return False

    islowercase = first_letter_is_lower_regex.match(title)
    if islowercase is not None:
        return False

    is_image_file = image_file_regex.match(title)
    if is_image_file:
        return False

    has_spaces = title.find(' ')
    if has_spaces > -1:
        return False

    if title in blacklist:
        return False

    return True

def filter():
    pass


def process_day(filename, dateStr):
    """
    pagecounts are stored in the following way:
        pagecounts/raw/YYYY/YYYY-MM/pagecounts-YYYYMMDD-HH0000.gz
    Each line in said file is space delimited and of the format
        <project> <title> <views> <bytes>
        e.g. en Barack_Obama 25 124 or af 2007_Rugby_W%C3%AAreldbeker 1 1
    """
    # takes in the 24 hourly files for specific date, and paralleizes them
    lines = sc.textFile(filename)

    parts = lines \
        .map(lambda l: l.split(" ")) \
        .filter(lambda line: line[0] == "en") \
        .filter(lambda line: len(line) > 3) \
        .cache()

    wiki = parts \
        .map(lambda a: Row(project = a[0],
                           title = unquote_plus(a[1]).lower(),
                           num_requests = int(a[2]),
                           resp_bytes = int(a[3])))

    wikiDf = session.createDataFrame(wiki)
    # so we can run standard SQL queries on the DataFrame
    wikiDf.registerTempTable("pagecounts")
    res = session.sql("SELECT '" + dateStr + "' AS date, title, count(*) AS cnt,"
                               "sum(num_requests) as tot_requests "
                      "FROM  pagecounts "
                      "GROUP BY title")

    jdbcUrl = "jdbc:mysql://localhost:3306/wikistats?user=wikistats&password=passwd"
    res.write.jdbc(url=jdbcUrl, table="daily_pagecounts", mode="append",
                   properties={"driver": 'com.mysql.jdbc.Driver'})




if __name__ == "__main__":

    BASE_DIR = "/media/william/Passport/pagecounts/raw/"
    conf = SparkConf().setAppName("Wiki Stats").setMaster("local")
    sc = SparkContext(conf=conf)
    session = SparkSession \
        .builder \
        .appName("Pagecounts Aggregator") \
        .getOrCreate()

    currDate = date(2007, 12, 10)
    endDate = date(2007, 12, 12)
    delta = timedelta(days = 1)

    while currDate < endDate:
        print("Aggregating hourly logs for "
              "{0}".format(currDate.strftime("%m-%d-%Y")))

        filePaths = BASE_DIR + currDate.strftime("%Y") + "/" + \
                    currDate.strftime("%Y-%m") + "/" \
                    "pagecounts-" + currDate.strftime("%Y%m%d") + \
                    "-*.gz"

        print(filePaths)
        process_day(filePaths, currDate.strftime("%Y-%m-%d"))
        currDate += delta










