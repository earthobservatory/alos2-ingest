#!/usr/bin/env python
"""
Ingest ALOS2 METADATA into ARIA-SG-NTU GRQ from ALOS2 raw data (extracted zip files) with:

  1) specific directory where ALOS2 raw is stored
  2) date of data
  3) ancillary information required for ingest_dataset.pu to work

"""
import alos2_utils
import logging
import glob
import os
import json
import argparse
import traceback
import subprocess as sp
import time
import shutil

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
    parser.add_argument('-dsfile', dest='ds_file', type=str, default='~/hysds/datasets.json',
            help = 'datasets.json file for ingestion into ARIA')
    parser.add_argument('-hysdsdir', dest='hysds_dir', type=str, default='~/hysds',
                        help='directory of hysds repo where ingest_dataset.py is kept')
    return parser.parse_args()

if __name__ == "__main__":
    args = cmdLineParse()

    try:
        data_files = sorted(glob.glob(os.path.join(args.dir, '*{}*'.format(args.date))))
        temp_dir = "tmp_{}_{}".format(args.date, int(time.time()))
        os.makedirs(temp_dir)
        for f in data_files:
            os.symlink(f, os.path.join(temp_dir, os.path.basename(f)))

        os.chdir(temp_dir)
        raw_dir = "."
        dataset_name = alos2_utils.extract_dataset_name(raw_dir) + "-md"
        logging.info("Creating metadata for {}.".format(dataset_name))
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
        ingest_script = os.path.join(args.hysds_dir, "scripts/ingest_dataset.py")
        logging.info("Ingesting {} into ARIA.".format(proddir))
        sp.check_call("{} {}/{} {}".format(ingest_script,temp_dir,proddir, args.ds_file), shell=True)

        #cleanup
        logging.info("Ingestion of {} complete. Cleaning up {} directory.".format(proddir,temp_dir))
        shutil.rmtree(temp_dir)

    except Exception as e:
        logging.warning("Ingestion might have failed. Check ARIA!")
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
