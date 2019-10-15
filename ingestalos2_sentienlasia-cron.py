#!/usr/bin/env python3
"""
Ingest ALOS2 data from a source to a destination:

  1) download data from a source and verify,
  2) extracts data and creates metadata
  3) push data to repository


HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.
"""

import logging, traceback, argparse, os, json
import alos2_productize
import scripts.sentinelasia_download as sa
import subprocess as sp
from datetime import datetime

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
PGE_PATH = os.path.dirname(__file__)

def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser( description='Getting ALOS-2 L2.1 / L1.1 data into ARIA')
    parser.add_argument('-start_time', action="store", dest="start_time", default="", required=True, help='Get the list of EORs and files  based on start day, YYYYMMDDD')
    parser.add_argument('-end_time', action="store", dest="end_time", default=datetime.today().strftime('%Y%m%d'), required=False, help='Get the list of EORs and files based on end day, YYYYMMDDD, defaults to today')
    parser.add_argument('-t','--tag', action="store", dest="tag", default="release-20191015",help='Release version to submit job-ingest_alos2_sentinelasia download job')
    parser.add_argument('-u','--username', action="store", dest="username", help='Sentinel Asia Login, if not givem, checks .netrc')
    parser.add_argument('-p','--password', action="store", dest="password", help='Sentinel Asia Login, if not givem, checks .netrc')
    parser.add_argument('-dry_run', action='store_true', dest="dry_run", default=False, help='Will not downlaod files if flag is defined')
    return parser.parse_args()

def submit_sa_data_download(data_id, queue, job_type):
    params = [
        {
            "name": "data_id",
            "from": "value",
            "value": data_id
        },
        {
            "name": "username",
            "from": "value",
            "value": ""
        },
        {
            "name": "password",
            "from": "value",
            "value": ""
        },
        {
            "name": "eor_id",
            "from": "value",
            "value": ""
        },
        {
            "name": "queue_eor_id",
            "from": "value",
            "value": ""
        },
        {
          "name": "script",
          "from": "value",
          "value": "ingestalos2_sentinelasia.py"
        }
    ]

    rule = {
        "rule_name": job_type.lstrip('job-'),
        "queue": queue,
        "priority": '5',
        "kwargs": '{}'
    }
    return rule, params


if __name__ == "__main__":
    args = cmdLineParse()

    try:
        # Remove all need for eor and data id
        args.eor_id = ""
        args.data_id = ""
        download_params = sa.get_all_params(args)

        if not args.dry_run:
            # for loop download split into 1 download = 1 job if only eor_id is specified
            for param in download_params:
                data_id = param["download_url"].rsplit('=', 1)[-1]
                queue = "aria-job_worker-large"
                tag = args.tag
                job_type = "job-ingest_alos2_sentinelasia"
                job_spec = "{}:{}".format(job_type, tag)
                rtime = datetime.utcnow()
                job_name = "%s-%s-%s" % (job_spec, data_id, rtime.strftime("%d_%b_%Y_%H:%M:%S"))
                rule, params = submit_sa_data_download(data_id, queue, job_type)
    
                command = PGE_PATH + '/submit_job.py --job_name %s --job_spec %s --params \'%s\' --rule \'%s\'' \
                          % (job_name, job_spec, json.dumps(params), json.dumps(rule))

                print("submitting job: "+ command)
                sp.check_call(command, shell=True)


    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
