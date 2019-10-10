#! /usr/bin/env python3

import requests
import argparse
import os
import time



LOGIN_URL = 'https://sentinel.tksc.jaxa.jp/sentinel2/topControl.jsp'
CATALOG_URL = 'https://sentinel.tksc.jaxa.jp/sentinel2/webresources/thumbnailEmob/emergencyViewThumbnail?requestId=ERJPYU000001&subsetName=Emergency+Observation&selectDate='
DL_URL = 'https://sentinel.tksc.jaxa.jp/sentinel2/webresources/thumbnailEmob/download?dataId='

def parse():
    '''Command line parser.'''
    desc = """Command line client for downloading ALOS-2 data from Sentinel-Asia EORs"""
    usage = """Example:
            For all ALOS2 Data in an EOR: sentinelasia_download.py -eor_id EOR_ID -u USERNAME -p PASSWORD
            For single ALOS2 Data in an EOR: sentinelasia_download.py -data_id Data_ID -u USERNAME -p PASSWORD"""
    parser = argparse.ArgumentParser(description=desc,usage=usage)
    parser.add_argument('-eor_id', action="store", dest="eor_id", default="", required=False, help='This is the EOR ID')
    parser.add_argument('-data_id', action="store", dest="data_id", default="", required=False, help='This is the Data ID')
    parser.add_argument('-u','--username', action="store", dest="username", default="", help='Sentinel Asia Login, if not givem, checks .netrc')
    parser.add_argument('-p','--password', action="store", dest="password", default="", help='Sentinel Asia Login, if not givem, checks .netrc')
    parser.add_argument('-dry_run', action='store_true', dest="dry_run", default=False, help='Will not downlaod files if flag is defined')
    inps = parser.parse_args()
    return inps

def session_login(username="", password=""):
    if not (username or password):
        creds = requests.utils.get_netrc_auth(LOGIN_URL)
        if creds is None:
            print("Please specify username and password, credentials cannot be found in .netrc")
            exit(0)
        username, password = creds

    with requests.Session() as s:
        payload = {'passwd': password, 'request': 'login', 'userid': username, 'submit': 'login', 'loginId': ''}
        s.get(LOGIN_URL)
        header_cookie = s.cookies.get_dict()
        print(header_cookie)
        # Spoof some of the headers fo requestig
        s.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:60.0) Gecko/20100101 Firefox/60.0'
        s.headers['Cookie'] = "JSESSIONID=" + header_cookie['JSESSIONID']
        r_login = s.post(LOGIN_URL, data=payload)
        r_login.encoding = 'utf-8'
        print("Login status code:  {}".format(r_login.status_code))

    return s



def get_download_urls(inps):

    s = session_login(inps.username, inps.password)

    # get list of files
    download_urls = []
    if inps.eor_id:
        r_catalog = s.get(CATALOG_URL)
        print("Catalog status code:  {}".format(r_catalog.status_code))
        print("Catalog response: {}".format(r_catalog.json()))

        if r_catalog.status_code == 200:
            catalog = r_catalog.json()
            for entry in catalog:
                if "ALOS(Data)" in entry["typeStr"]:
                    download_urls.append(DL_URL + entry["dataId"])
                    print("Found ALOS(Data):{} in EOR: {} ".format(entry["dataId"],inps.eor_id))
    else:
        download_urls = [DL_URL + inps.data_id]

    return download_urls

def get_file_params(inps, dl_url):
    filename = ""
    filesize = 0

    s = session_login(inps.username, inps.password)

    r_file_check = s.head(dl_url)
    print("File check status code: {}".format(r_file_check.status_code))
    if r_file_check.status_code == 200:
        print("File check headers: {}".format(r_file_check.headers))
        filename = r_file_check.headers['Content-Disposition'].split("=")[-1].strip().replace('"', '')
        filesize = int(r_file_check.headers['Content-Length'])


    return filename, filesize


def do_download(inps, download_urls):
    s = session_login(inps.username, inps.password)

    if not inps.dry_run:
            # TODO: parallelize this!
            for dl_url in download_urls:
                r_download_check = s.head(dl_url)
                print("Download check status code: {}".format(r_download_check.status_code))
                if r_download_check.status_code == 200:
                    print("Download check headers: {}".format(r_download_check.headers))

                    with s.get(dl_url, stream=True) as r_download:
                        r_download.raise_for_status()
                        o_file = r_download_check.headers['Content-Disposition'].split("=")[-1].strip().replace('"', '')
                        # download file
                        print("Downloading file to: {}".format(o_file))

                        with open(o_file, 'wb') as f:
                            count = 0
                            start = time.time()
                            CHUNK = 256 * 1024

                            for chunk in r_download.iter_content(chunk_size=CHUNK):
                                count += 1
                                if chunk:  # filter out keep-alive new chunks
                                    f.write(chunk)
                                    if not count % 20:
                                        print("Wrote %s chunks: %s MB " % (count, str(count * CHUNK / (1024 * 1024))))
                            f.close()
                            total_time = time.time() - start
                            mb_sec = (os.path.getsize(o_file) / (1024 * 1024.0)) / total_time
                            print("Speed: %s MB/s" % mb_sec)
                            print("Total Time: %s s" % total_time)

                        r_download.close()



if __name__ == '__main__':
    # Session will be closed at the end of with block
    inps = parse()
    if not (inps.eor_id or inps.data_id):
        print("Please specify either eor_id or data_id")
        exit(0)


    download_urls = get_download_urls(inps)
    do_download(inps, download_urls)
