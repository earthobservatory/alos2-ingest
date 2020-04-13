#!/usr/bin/env python3
"""
Ingest ALOS2 data from a source to a destination:

  1) download data from a source and verify,
  2) extracts data and creates metadata
  3) push data to repository


HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.
"""

import logging, traceback, argparse
import scripts.auig2_download as auig2
import alos2_productize
import base64

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser( description='Getting ALOS-2 L2.1 / L1.1 data into ARIA from AUIG2')
    parser.add_argument('-o', dest='order_id', type=str, default='',
            help = 'Order ID from AUIG2 if available')
    parser.add_argument('-u', dest='username', type=str, default='',
            help = 'Usernmae from AUIG2 if available')
    parser.add_argument('-p', dest='password', type=str, default='',
            help = 'Password from AUIG2 if available')

    return parser.parse_args()

if __name__ == "__main__":
    args = cmdLineParse()
    ctx = alos2_productize.load_context()

    try:
        # first check if we need to read from _context.json
        if not (args.username and args.password):
            # no inputs defined (as per defaults)
            # we need to try to load from context
            args.username = ctx["auig2_username"]
            decode = lambda pw: ''.join([chr(ord(pw[i]) - (i%3 - 1 )) for i in range(len(pw))])
            args.password = decode(ctx["auig2_password"])

        if not args.order_id:
            args.order_id = ctx["auig2_orderid"]

        # TODO: remember to bring back the download
        url = auig2.download(args)
        download_source = url
        alos2_productize.ingest_alos2(download_source)

    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
