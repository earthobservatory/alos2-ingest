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
            fdates=[]
            for x in files:
                m = re.search("IMG-[A-Z]{2}-ALOS2.{05}(.{04}-\d{6})-.{4}1.1.*", x)
                if m:
                    fdates.append(m.group(1))

            fdates_unique = set(fdates)
            fdates_unique = list(fdates_unique)
            print("Frame Dates: {}".format(fdates_unique))

            for fdate in fdates_unique:
                folder_struct = re.search(regex, root)

                if folder_struct:
                    print("submitting job for root:{} frame_date:{}}".format(root,fdate))
                    sp.check_call("qsub {} -v dir={},fdate={} -N {} ".format(args.pbsfile,root,fdate,fdate),shell=True)


        # ignore root and subFolders
        # get all of the files that resembles IMG file regex,
        # get a unique set of their dates
        # submit to qsub with path name
