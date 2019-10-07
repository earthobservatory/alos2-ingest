#! /usr/bin/env python3
###############################################################################
#  auig2_download.py
#
#  Purpose:  Command line download from AUIG2
#  Author:   Scott Baker
#  Created:  Apr 2015
#
###############################################################################
#  Copyright (c) 2015, Scott Baker
#
#  Permission is hereby granted, free of charge, to any person obtaining a
#  copy of this software and associated documentation files (the "Software"),
#  to deal in the Software without restriction, including without limitation
#  the rights to use, copy, modify, merge, publish, distribute, sublicense,
#  and/or sell copies of the Software, and to permit persons to whom the
#  Software is furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included
#  in all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
#  OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
#  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.
###############################################################################
# Adapted from:
# https://github.com/bakerunavco/Archive-Tools/blob/master/alos2/auig2_download.py

import os
import sys
import time
import datetime
import urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
import http.cookiejar
import argparse


BASE_URL = 'https://auig2.jaxa.jp/openam/UI/Login'
USERNAME='' # YOUR USERNAME CAN ALOS BE HARDWIRED HERE
PASSWORD='' # YOUR PASSWORD CAN ALOS BE HARDWIRED HERE

def loginToAUIG2(opener,inps):
    """
    Handle login. This should populate our cookie jar.
    """
    login_data = urllib.parse.urlencode({
        'IDToken1' : inps.username,
        'IDToken2' : inps.password,
    }).encode("utf-8")
    response = opener.open(BASE_URL, login_data)
    return response.read().decode("utf-8")

def parse():
    '''Command line parser.'''
    desc = """Command line client for downloading from AUIG2
For questions or comments, contact Scott Baker: baker@unavco.org
    """
    epi = """You can hardwire your AUIG2 USERNAME and PASSWORD in this file (it's near the top), or use command line args"""
    usage = """Example:
auig2_download.py -o ORDER_ID -u USERNAME -p PASSWORD
If you have your credentials hardwired in this file, just do:
auig2_download.py -o ORDER_ID
"""
    parser = argparse.ArgumentParser(description=desc,epilog=epi,usage=usage)
    parser.add_argument('-o','--orderid', action="store", dest="order_id", metavar='<ORDERID>', required=True, help='This is your AUIG2 Order ID')
    parser.add_argument('-u','--username', action="store", dest="username", metavar='<USERNAME>', default=USERNAME, help='AUIG2 Login')
    parser.add_argument('-p','--password', action="store", dest="password", metavar='<PASSWORD>', default=PASSWORD, help='AUIG2 Login')
    inps = parser.parse_args()
    return inps

def download(inps):
    ### OPEN A CONNECTION TO AUIG2 AND LOG IN ###
    cookie_filename = "cookie_%s.txt" % datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    cj = http.cookiejar.MozillaCookieJar(cookie_filename)
    if os.access(cookie_filename, os.F_OK):
        cj.load()
    opener = urllib.request.build_opener(
        urllib.request.HTTPRedirectHandler(),
        urllib.request.HTTPHandler(debuglevel=0),
        urllib.request.HTTPSHandler(debuglevel=0),
        urllib.request.HTTPCookieProcessor(cj)
    )
    opener.addheaders = [('User-agent', ('Mozilla/4.0 (compatible; MSIE 6.z0; ' 'Windows NT 5.2; .NET CLR 1.1.4322)'))]
    # need this twice - once to set cookies, once to log in...
    loginToAUIG2(opener, inps)
    loginToAUIG2(opener, inps)

    ### DOWNLOAD THE FILE WITH THE GIVEN ORDER ID ###
    url = "http://auig2.jaxa.jp/pp/service/download?downloadurl=/start/download/file&itemname=%s&itemtype=1" % inps.order_id
    f = opener.open(url)
    print("Downloading from url: %s " % url)
    print("Header reply: %s " % f.headers['Content-Disposition'])
    filename = f.headers['Content-Disposition'].split("=")[-1].strip()
    print("ALOS-2 AUIG2 Download:", filename)
    start = time.time()
    CHUNK = 256 * 1024
    # meta = f.info()
    filesize = f.getheader("Content-Length")
    print("Content-Length: %s" % filesize)
    count = 0

    with open(filename, 'wb') as fp:
        while True:
            count += 1
            chunk = f.read(CHUNK)
            if not chunk: break
            fp.write(chunk)
            if not count % 20:
                print("Wrote %s chunks: %s MB " % (count, str(count * CHUNK / (1024 * 1024))))
    f.close()
    total_time = time.time() - start
    mb_sec = (os.path.getsize(filename) / (1024 * 1024.0)) / total_time
    print("Speed: %s MB/s" % mb_sec)
    print("Total Time: %s s" % total_time)

    return url

if __name__ == '__main__':
    if len(sys.argv)==1:
        sys.argv.append('-h')
    ### READ IN PARAMETERS FROM THE COMMAND LINE ###
    inps = parse()

    download(inps)
