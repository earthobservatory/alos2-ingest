#!/usr/bin/env python
"""
Ingest ALOS2 data from a source to a destination:

  1) download data from a source and verify,
  2) extracts data and creates metadata
  3) push data to repository


HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.
"""
import alos2_utils
import logging
import glob
import os
import json
import argparse
import traceback

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser( description='Getting ALOS-2 data in gekko into ARIA')
    parser.add_argument('-dir', dest='dir', type=str, default='',
            help = 'directory to search ALOS2 files')
    parser.add_argument('-date', dest='date', type=str, default='',
            help = 'date of ALOS2 files in YYMMDD format')
    return parser.parse_args()

if __name__ == "__main__":
    args = cmdLineParse()

    try:
        data_files = sorted(glob.glob(os.path.join(args.dir, '*{}*'.format(args.date))))
        temp_dir = args.date
        os.makedirs(temp_dir, exist_ok=True)
        for f in data_files:
            os.symlink(f, os.path.join(temp_dir, os.path.basename(f)))

        os.chdir(temp_dir)
        raw_dir = "."
        dataset_name = alos2_utils.extract_dataset_name(raw_dir) + "-md"
        is_l11 = alos2_utils.ALOS2_L11 in dataset_name
        metadata, dataset, proddir = alos2_utils.create_product_base(raw_dir, dataset_name, is_l11)

        # add metadata
        metadata["gekko_archive_files"] = data_files

        # dump metadata
        with open(os.path.join(proddir, dataset_name + ".met.json"), "w") as f:
            json.dump(metadata, f, indent=2)
            f.close()

        # dump dataset
        with open(os.path.join(proddir, dataset_name + ".dataset.json"), "w") as f:
            json.dump(dataset, f, indent=2)
            f.close()

        os.chdir("..")
        #ingest

        #cleanup

    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
