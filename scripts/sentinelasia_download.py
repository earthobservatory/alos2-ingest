#! /usr/bin/env python3

import requests
import argparse
import os
import time
import json
from datetime import datetime


LOGIN_URL = 'https://sentinel.tksc.jaxa.jp/sentinel2/topControl.jsp'
EOR_LIST_URL = 'https://sentinel.tksc.jaxa.jp/sentinel2/webresources/emobRequestSelect/viewList?subset_name=Emergency+Observation&submit.countryIdx=&submit.disasterTypeIdx='
EOR_ID_FILES = 'https://sentinel.tksc.jaxa.jp/sentinel2/webresources/thumbnailEmob/emergencyViewThumbnail?requestId={}&subsetName=Emergency+Observation&selectDate='
EOR_ID_BULLETIN = 'https://sentinel.tksc.jaxa.jp/sentinel2/webresources/thumbnailEmob/viewBulletinContent?requestId={}'
DL_URL = 'https://sentinel.tksc.jaxa.jp/sentinel2/webresources/thumbnailEmob/download?dataId='

def parse():
    '''Command line parser.'''
    desc = """Command line client for downloading ALOS-2 data from Sentinel-Asia EORs"""
    usage = """Example:
            For all ALOS2 Data in an EOR: sentinelasia_download.py -eor_id EOR_ID -u USERNAME -p PASSWORD
            For single ALOS2 Data in an EOR: sentinelasia_download.py -data_id Data_ID -u USERNAME -p PASSWORD"""
    parser = argparse.ArgumentParser(description=desc,usage=usage)
    parser.add_argument('-eor_id', action="store", dest="eor_id", default="", required=False, help='Used with either -d or -l. EOR_ID to list or download. ')
    parser.add_argument('-data_id', action="store", dest="data_id", default="", required=False, help='Used with only -l for listing. ')
    parser.add_argument('-start_time', action="store", dest="start_time", default="", required=False, help='Get the list of EORs and files  based on start day, YYYYMMDDD')
    parser.add_argument('-end_time', action="store", dest="end_time", default=datetime.today().strftime('%Y%m%d'), required=False, help='Get the list of EORs and files based on end day, YYYYMMDDD, defaults to today')
    parser.add_argument('-dry_run', action='store_true', dest="dry_run", default=False, help='Will not downlaod files if flag is defined')
    parser.add_argument('-u','--username', action="store", dest="username", default="", help='Sentinel Asia Login, if not given, checks .netrc')
    parser.add_argument('-p','--password', action="store", dest="password", default="", help='Sentinel Asia Login, if not given, checks .netrc')
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



def get_all_params(inps):
    s = session_login(inps.username, inps.password)

    all_params = []
    if inps.eor_id:
        all_params = get_eorid_allfiles(inps, inps.eor_id, session=s)
    elif inps.data_id:
        all_params = [get_file_params(inps, inps.data_id, s)]
    elif inps.start_time:
        eor_list = get_eor_list(inps, s)
        for eor_id_bulletin in eor_list:
            eor_date = datetime.strptime(eor_id_bulletin["observeDateStr"],'%d/%b/%Y')
            start = datetime.strptime(inps.start_time, '%Y%m%d')
            end = datetime.strptime(inps.end_time, '%Y%m%d')
            eor_id = eor_id_bulletin["requestId"]
            if eor_date >= start and eor_date <= end:
                all_params.extend(get_eorid_allfiles(inps, eor_id, eor_id_bulletin, s))

    # print statement here
    if not all_params:
        print("Unable to find anything in this search!")
    else:
        print("Found {} ALOS-2 Data entries for your search:".format(len(all_params)))
        # print(json.dumps(download_params, indent=4, sort_keys=True))

    for param in all_params:
        param = param.copy()
        param.pop("download_url")
        print(json.dumps(param, sort_keys=True))

    return all_params

def get_eor_list(inps, session=None):
    s = session
    if not s:
         s = session_login(inps.username, inps.password)

    r_eor_catalog = s.get(EOR_LIST_URL)
    print("EOR list status code:  {}".format(r_eor_catalog.status_code))
    if r_eor_catalog.status_code == 200:
        catalog = r_eor_catalog.json()
        # print(json.dumps(catalog, indent=2))
        return catalog
    else:
        raise RuntimeError("Unable to retrieve EOR List")


def get_eorid_allfiles(inps, eor_id, eor_id_bulletin=None, session=None):
    all_params = []
    s = session
    if not s:
         s = session_login(inps.username, inps.password)

    if not eor_id_bulletin:
        eor_id_bulletin = get_eorid_bulletin(inps, eor_id, s)

    eor_data = {"eor_id": eor_id_bulletin["requestId"],
                "eor_date": datetime.strptime(eor_id_bulletin["observeDateStr"],'%d/%b/%Y').strftime('%Y%m%d'),
                "eor_type": eor_id_bulletin["disasterTypeStr"],
                "eor_country": eor_id_bulletin["countryStr"]
                }

    r_data_catalog = s.get(EOR_ID_FILES.format(eor_id))
    print("EOR ID status code:  {}".format(r_data_catalog.status_code))

    if r_data_catalog.status_code == 200:
        catalog = r_data_catalog.json()
        # print(json.dumps(catalog, indent=2))
        for entry in catalog:
            if "ALOS(Data)" in entry["typeStr"]:
                data_id = entry["dataId"]
                eor_data.update({"filetitle":entry["title"]})
                file_params = get_file_params(inps, data_id, s)
                print("Found ALOS(Data):{} in EOR: {} ".format(data_id, eor_id))
                # join eor_data and file_params
                eor_data_cp = eor_data.copy()
                file_params.update(eor_data_cp)
                all_params.append(file_params)

    return all_params

def get_eorid_bulletin(inps, eor_id, session=None):
    s = session
    if not s:
         s = session_login(inps.username, inps.password)

    r_eorid = s.get(EOR_ID_BULLETIN.format(eor_id))
    print("EOR Bulletin status code:  {}".format(r_eorid.status_code))
    if r_eorid.status_code == 200:
        # print("EOR Bulletin response: {}".format(r_eorid.json()))
        return r_eorid.json()
    else:
        raise RuntimeError("Unable to retrieve EOR bulletin details")


def get_file_params(inps, data_id, session=None):
    s = session
    if not s:
         s = session_login(inps.username, inps.password)

    dl_url = DL_URL + data_id
    r_file_check = s.head(dl_url)
    print("File check status code: {}".format(r_file_check.status_code))
    if r_file_check.status_code == 200:
        # print("File check headers: {}".format(r_file_check.headers))
        filename = r_file_check.headers['Content-Disposition'].split("=")[-1].strip().replace('"', '')
        filesize = int(r_file_check.headers['Content-Length'])
        file_params = {"data_id":data_id, "download_url":dl_url,"filename":filename, "filesize":filesize}
        return file_params
    else:
        raise RuntimeError("Unable to retrieve file parameters")


def do_download(inps, download_params):
    s = session_login(inps.username, inps.password)
    # TODO: parallelize this!
    for param in download_params:
        dl_url = param['download_url']
        r_download_check = s.head(dl_url)
        print("Download check status code: {}".format(r_download_check.status_code))
        if r_download_check.status_code == 200:
            print("Download check headers: {}".format(r_download_check.headers))
            with s.get(dl_url, stream=True) as r_download:
                r_download.raise_for_status()
                o_file = r_download_check.headers['Content-Disposition'].split("=")[-1].strip().replace('"', '')
                # download file
                if not os.path.isfile(o_file):
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
    # Download commanded
    if not (inps.eor_id or inps.data_id or inps.start_time):
        print("Please specify either eor_id or data_id or start_time")
        exit(0)

    download_params = get_all_params(inps)

    if not inps.dry_run:
        print("Downloading data within search")
        do_download(inps, download_params)


