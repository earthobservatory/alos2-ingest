#! /usr/bin/env python3
###############################################################################
#  gportal_download.py
#
#  Purpose:  Command line download from GPortal
#  Authors:   Scott Baker (edited by Shi Tong Chin)
#  Created:  Apr 2015, updated Nov 2021
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
# please ensure you have the following installed:
# conda install -c conda-forge selenium
# conda install -c conda-forge phantomjs

import os
import sys
import time
import datetime
import urllib.request, urllib.parse, urllib.error
import shutil
import requests
import argparse
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

LOGIN_URL = 'https://gportal.jaxa.jp/gpr/auth'
MAXIMUM_LOOPS = 5
CHUNK = 256 * 1024
USERNAME = ''  # YOUR USERNAME CAN ALOS BE HARDWIRED HERE
PASSWORD = ''  # YOUR PASSWORD CAN ALOS BE HARDWIRED HERE


def selenium_login(driver, link, un, pw):
    driver.get(link)

    username = driver.find_element_by_id("auth_account")
    password = driver.find_element_by_id("auth_password")

    username.send_keys(un)
    password.send_keys(pw)

    driver.find_element_by_id("auth_login_submit").click()
    time.sleep(5)


def parse():
    '''Command line parser.'''
    desc = """Command line client for downloading from GPORTAL
For questions or comments, contact Scott Baker: baker@unavco.org
    """
    epi = """You can hardwire your GPORTAL USERNAME and PASSWORD in this file (it's near the top), or use command line args"""
    usage = """Example:
gportal_download.py -o ORDER_ID -u USERNAME -p PASSWORD
If you have your credentials hardwired in this file, just do:
gportal_download.py -o ORDER_ID
"""
    parser = argparse.ArgumentParser(description=desc, epilog=epi, usage=usage)
    parser.add_argument('-l', '--download_link', action="store", dest="download_link", metavar='<LINK>', required=True,
                        help='This is your GPortal download link')
    parser.add_argument('-u', '--username', action="store", dest="username", metavar='<USERNAME>', default=USERNAME,
                        help='GPortal Login')
    parser.add_argument('-p', '--password', action="store", dest="password", metavar='<PASSWORD>', default=PASSWORD,
                        help='GPortal Login')
    inps = parser.parse_args()
    return inps



def get_request_cookies(inps):
    driver = webdriver.PhantomJS()
    selenium_login(driver=driver, link=LOGIN_URL, un=inps.username, pw=inps.password)
    driver_cookies = driver.get_cookies()
    driver.close()
    print(driver_cookies)
    cookie = {cookie['name']: cookie['value'] for cookie in driver_cookies}
    print(cookie)
    return cookie

def get_timenow():
    dateTimeObj = datetime.datetime.now()
    timestampStr = dateTimeObj.strftime("[%Y-%m-%d %H:%M:%S.%f]")

    return timestampStr


def download(inps):
    cookie = get_request_cookies(inps)
    if 'goto=' in inps.download_link:
        download_link = inps.download_link.split("goto=")[-1]
    else:
        download_link = inps.download_link

    filesize_b = int(requests.head(download_link, cookies=cookie).headers['Content-Length'])
    a = urllib.parse.urlparse(download_link)
    print(f"Filesize is: {filesize_b} bytes")
    o_file = os.path.basename(a.path)  # file name with order id

    # download file
    if os.path.exists(o_file):
        temp_size = os.path.getsize(o_file)
        if filesize_b == temp_size:
            return
    else:
        temp_size = 0

    download_complete = temp_size == filesize_b

    for i in range(MAXIMUM_LOOPS):
        headers = {'Range': 'bytes=%d-' % temp_size}
        if temp_size:
            print(f"Headers to start mid-file: {headers}")
        with requests.Session() as s:
            # from: https://stackoverflow.com/questions/60849208/python-file-is-not-fully-downloaded-using-requests/67053532#67053532
            with s.get(download_link, stream=True, cookies=cookie, headers=headers) as r_download:
                if r_download.status_code < 300:
                    # continue to download only if requests passed
                    print(f"Downloading file to: {o_file} ({filesize_b / (1024 * 1024):.2f} MB)")
                    with open(o_file, 'ab') as fl:
                        # r_download.raw.decode_content = True
                        # shutil.copyfileobj(r_download.raw, f, length=16 * 1024 * 1024)
                        start = time.time()
                        count = 0
                        for chunk in r_download.iter_content(chunk_size=CHUNK):
                            count += 1
                            if chunk:  # filter out keep-alive new chunks
                                fl.write(chunk)
                                if not count % 20:
                                    size = count * CHUNK / (1024 * 1024)
                                    percent = count * CHUNK / filesize_b * 100
                                    print(f"{get_timenow()}: Wrote {count} chunks: {size} MB ({percent:.2f} %)")
                        fl.close()
                        total_time = time.time() - start
                        mb_sec = (os.path.getsize(o_file) / (1024 * 1024.0)) / total_time
                        print(f"{get_timenow()}: Speed: {mb_sec} MB/s")
                        print(f"{get_timenow()}: Total Time: {total_time} s")
                        temp_size = os.path.getsize(o_file)

        download_complete = temp_size == filesize_b

        if not download_complete:
            percent = temp_size / filesize_b * 100
            print(f"{get_timenow()}: Wrote  {temp_size} MB ({percent:.2f} %)")
            print(f"Download not completed somehow. "
                  f"\n Restarting download from where we left off: ({temp_size/(1024 * 1024):.2f} MB)")
        else:
            print(f"Download completed. Breaking loop. Wrote  {temp_size} MB")
            break

        cookie = get_request_cookies(inps)



if __name__ == '__main__':
    if len(sys.argv) == 1:
        sys.argv.append('-h')
    ### READ IN PARAMETERS FROM THE COMMAND LINE ###
    inps = parse()
    download(inps)
