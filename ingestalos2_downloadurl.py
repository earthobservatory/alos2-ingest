#!/usr/bin/env python3
"""
Ingest ALOS2 data from a source to a destination:

  1) download data from a source and verify,
  2) extracts data and creates metadata
  3) push data to repository


HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.
"""

import logging, traceback, argparse
import alos2_utils
import ingest_alos2_proto

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser( description='Getting ALOS-2 L2.1 / L1.1 data into ARIA')
    parser.add_argument('-d', dest='download_url', type=str, default='',
            help = 'Download url if available')
    parser.add_argument('-o', dest='order_id', type=str, default='',
            help = 'Order ID from AUIG2 if available')
    parser.add_argument('-u', dest='username', type=str, default='',
            help = 'Usernmae from AUIG2 if available')
    parser.add_argument('-p', dest='password', type=str, default='',
            help = 'Password from AUIG2 if available')
    parser.add_argument("--file_type", dest='file_type', help="download file type to verify", default='zip',
                        choices=alos2_utils.ALL_TYPES, required=False)
    return parser.parse_args()

if __name__ == "__main__":
    args = cmdLineParse()

    try:
        # first check if we need to read from _context.json
        if not args.download_url:
            # no inputs defined (as per defaults)
            # we need to try to load from context
            ctx = ingest_alos2_proto.load_context()
            args.download_url = ctx["download_url"]
            alos2_utils.download(args.download_url)
            download_source = args.download_url

        ingest_alos2_proto.ingest_alos2(download_source, args.file_type)

    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
