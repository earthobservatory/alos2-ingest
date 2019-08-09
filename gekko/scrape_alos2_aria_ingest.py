#!/usr/bin/env python3

import os
import logging
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
    parser.add_argument('-pbs', dest='pbsfile', type=str, default='',
            help = 'pbsfile to do ingestion')
    return parser.parse_args()

if __name__ == "__main__":
    args = cmdLineParse()
    for root, subFolders, files in os.walk(args.dir):
        if files:
            dates=[]
            for x in files:
                m = re.search("IMG-[A-Z]{2}-ALOS2.{09}-(\d{6})-.{4}1.1.*", x)
                if m:
                    dates.append(m.group(1))

            dates_unique = set(dates)
            dates_unique = list(dates_unique)

            for date in dates_unique:
                sp.check_call("qsub {} -v dir={},date={}".format(args.pbsfile,root,date),shell=True)


        # ignore root and subFolders
        # get all of the files that resembles IMG file regex,
        # get a unique set of their dates
        # submit to qsub with path name
