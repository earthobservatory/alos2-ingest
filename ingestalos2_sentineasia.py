#!/usr/bin/env python3
"""
Ingest ALOS2 data from a source to a destination:

  1) download data from a source and verify,
  2) extracts data and creates metadata
  3) push data to repository


HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.
"""

import logging, traceback, argparse, os, json
import alos2_utils
import ingest_alos2_proto
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
    parser.add_argument('-eor_id', action="store", dest="eor_id", default="", required=False, help='This is the EOR ID')
    parser.add_argument('-data_id', action="store", dest="data_id", default="", required=False, help='This is the Data ID')
    parser.add_argument('-u','--username', action="store", dest="username", help='Sentinel Asia Login, if not givem, checks .netrc')
    parser.add_argument('-p','--password', action="store", dest="password", help='Sentinel Asia Login, if not givem, checks .netrc')
    parser.add_argument('-dry_run', action='store_true', dest="dry_run", default=False, help='Will not downlaod files if flag is defined')
    parser.add_argument("--file_type", dest='file_type', help="download file type to verify", default='zip',
                        choices=alos2_utils.ALL_TYPES, required=False)
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
        # first check if we need to read from _context.json
        if not (args.eor_id or args.data_id):
            ctx = ingest_alos2_proto.load_context()
            args.eor_id = ctx["eor_id"]
            args.data_id = ctx["data_id"]


        if args.eor_id and args.data_id:
            raise RuntimeError("Please only specify either data_id or eor_id, do not specify both!")
        else:
            download_urls = sa.get_download_urls(args)

        if args.data_id:
            # only 1 download if only data_id is specified
            # sa.do_download(args, download_urls)
            download_source = download_urls[0]
            ingest_alos2_proto.ingest_alos2(download_source, args.file_type)

        else:
            # for loop download if only eor_id is specified
            for download_url in download_urls:
                data_id = download_url.rsplit('/', 1)[-1]
                queue = ctx["queue_eor_id"]
                tag = ctx['job_specification']['job-version']
                job_type = "job-ingest_alos2_sentinelasia"
                job_spec = "{}:{}".format(job_type, tag)
                rtime = datetime.utcnow()
                job_name = "%s-%s-%s" % (job_spec, data_id, rtime.strftime("%d_%b_%Y_%H:%M:%S"))
                rule, params = submit_sa_data_download(data_id, queue, job_type)

                command = PGE_PATH + '/submit_job.py --job_name %s --job_spec %s --params \'%s\' --rule \'%s\' > temp.log' \
                          % (job_name, job_spec, json.dumps(params), json.dumps(rule))

                print("submitting job: "+ command)
                sp.check_call(command, shell=True)


    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise