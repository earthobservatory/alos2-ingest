#!/usr/bin/env python3

import os
import argparse
import re
import subprocess as sp
def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser( description='Scraping gekko for ALOS-2 data and ingesting it into ARIA')
    parser.add_argument('-dir', dest='dir', type=str, default='',
            help = 'directory to scrape ALOS2 files')
    parser.add_argument('-regex', dest='regex', type=str, default=".*/P\d{3}/F\d{4}.*",
            help = 'regular expression to match folder structure where ALOS2 data is stored. Leave "" for none')
    parser.add_argument('-pbs', dest='pbsfile', type=str, default='',
            help = 'pbsfile to do ingestion')
    return parser.parse_args()


if __name__ == "__main__":
    args = cmdLineParse()

    if not args.regex:
        # match anything
        regex = ".*"
    else:
        regex = args.regex

    for root, subFolders, files in os.walk(args.dir):
        if files:
            dates=[]
            for x in files:
                m = re.search("IMG-[A-Z]{2}-ALOS2.{09}-(\d{6})-.{4}1.1.*", x)
                if m:
                    dates.append(m.group(1))

            dates_unique = set(dates)
            dates_unique = list(dates_unique)
            print("Dates: {}".format(dates_unique))

            for date in dates_unique:
                folder_struct = re.search(regex, root)


                if folder_struct:
                    name = "{}/{}".format(root,date)
                    name.replace("/", "_")
                    print("submitting job for root,{} date,{}: {}".format(root,date, name))
                    sp.check_call("qsub {} -v dir={},date={} -N {} ".format(args.pbsfile,root,date,name),shell=True)


        # ignore root and subFolders
        # get all of the files that resembles IMG file regex,
        # get a unique set of their dates
        # submit to qsub with path name
