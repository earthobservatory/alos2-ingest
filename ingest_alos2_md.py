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
import requests
from hysds.celery import app

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

def check_dataset(es_url, id, es_index="grq"):
    """Query for dataset with specified input ID."""

    query = {
        "query":{
            "bool":{
                "must":[
                    {"term":{"_id":id}},
                ]
            }
        },
        "fields": [],
    }

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code == 200:
        result = r.json()
        #logging.info("result: %s" % result)
        total = result['hits']['total']
        id = 'NONE' if total == 0 else result['hits']['hits'][0]['_id']
    else:
        logging.error("Failed to query %s:\n%s" % (es_url, r.text))
        logging.error("query: %s" % json.dumps(query, indent=2))
        logging.error("returned: %s" % r.text)
        if r.status_code == 404: total, id = 0, 'NONE'
        else: r.raise_for_status()
    return total, id

if __name__ == "__main__":
    args = cmdLineParse()
    grq_es_url = app.conf['GRQ_ES_URL']

    try:
        data_files = sorted(glob.glob(os.path.join(args.dir, '*{}*'.format(args.date))))
        temp_dir = "tmp_{}_{}".format(args.dir.replace("/", "_"),args.date)
        os.makedirs(temp_dir)
        for f in data_files:
            os.symlink(f, os.path.join(temp_dir, os.path.basename(f)))

        os.chdir(temp_dir)
        raw_dir = "."
        dataset_name = alos2_utils.extract_dataset_name(raw_dir) + "-md"

        total, id = check_dataset(grq_es_url, dataset_name)
        logging.info("In GRQ - id: {} total: {}.".format(id, total))

        if total > 0:
            logging.info("Not ingesting {} as it is present in GRQ as {}.".format(dataset_name, id))
            logging.info("Cleaning up {} directory.".format(temp_dir))
            shutil.rmtree(temp_dir)
        else:
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
        logging.warning("Ingestion of {} with date:{} might have failed. Check ARIA!".format(args.dir,args.date))
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise

