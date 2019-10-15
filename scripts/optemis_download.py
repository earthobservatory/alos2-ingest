#! /usr/bin/env python3
import requests
import argparse
import os
import time
import json
from datetime import datetime
import re

LOGIN_URL = 'https://optemis.sentinel-asia.org/dashboard/users/login'
SIGNIN_URL = 'https://optemis.sentinel-asia.org/dashboard/users/signin'
EOR_MANAGEMENT_URL = 'https://optemis.sentinel-asia.org/ajax/v1/emergency_requests?draw=1&columns[0][data]=index' \
                     '&columns[0][name]=&columns[0][searchable]=true&columns[0][orderable]=true&columns[0][search][value]=&columns[0][search][regex]=false' \
                     '&columns[1][data]=country&columns[1][name]=&columns[1][searchable]=true&columns[1][orderable]=true&columns[1][search][value]=&columns[1][search][regex]=false' \
                     '&columns[2][data]=requester&columns[2][name]=&columns[2][searchable]=true&columns[2][orderable]=true&columns[2][search][value]=&columns[2][search][regex]=false' \
                     '&columns[3][data]=disaster&columns[3][name]=&columns[3][searchable]=true&columns[3][orderable]=true&columns[3][search][value]=&columns[3][search][regex]=false&columns[4][data]=map' \
                     '&columns[4][name]=&columns[4][searchable]=true&columns[4][orderable]=true&columns[4][search][value]=&columns[4][search][regex]=false' \
                     '&columns[5][data]=date&columns[5][name]=&columns[5][searchable]=true&columns[5][orderable]=true&columns[5][search][value]=&columns[5][search][regex]=false' \
                     '&columns[6][data]=period&columns[6][name]=&columns[6][searchable]=true&columns[6][orderable]=true&columns[6][search][value]=&columns[6][search][regex]=false' \
                     '&columns[7][data]=status&columns[7][name]=&columns[7][searchable]=true&columns[7][orderable]=true&columns[7][search][value]=&columns[7][search][regex]=false' \
                     '&columns[8][data]=actions&columns[8][name]=&columns[8][searchable]=true&columns[8][orderable]=true&columns[8][search][value]=&columns[8][search][regex]=false' \
                     '&order[0][column]=5' \
                     '&order[0][dir]=desc' \
                     '&start=0' \
                     '&length=10' \
                     '&search[value]=' \
                     '&search[regex]=false' \
                     '&_=1570768807950'

SHAPEFILEINFO_URL = 'https://optemis.sentinel-asia.org/ajax/v1/emergency_requests/5d9448aae4a9124f601fd7be/dpn/shapefiles'
ZIPFILEINFO_URL = 'https://optemis.sentinel-asia.org/ajax/v1/emergency_requests/5d9448aae4a9124f601fd7be/dpn/datafiles/organizations/59279239aa73bf3eac0056b4'
DOWNLOAD_URL = 'https://optemis.sentinel-asia.org/dashboard/downloadFile?file=big-storage/Sentinel-Asia/Emergency_Response/20191001-Taiwan-Other-00142/PLAN/Japan Aerospace Exploration Agency (JAXA)/TaiwanALOS2Shape_a87500efcb4ded1b6fd9a9a2d5ae0b6d.zip'

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
    payload = {'password': password, 'email': username, '_token': ''}
    r_login_page = requests.get(LOGIN_URL, verify=True)
    token = ""
    # Get the _token field to update the payload
    if r_login_page.status_code == 200:
        re_csrf = re.compile('.*<meta name="csrf-token" content="(.*)".*')
        for line in r_login_page.text.splitlines():
            match = re_csrf.search(line)
            if match:
                token = match.group(1)
                break
        payload.update({'_token': token})
    else:
        raise RuntimeError("Unable to get page and update token")
    with requests.Session() as s:
        s.get(LOGIN_URL, verify=True)
        header_cookie = s.cookies.get_dict()
        print(header_cookie)
        # Spoof some of the headers fo requestig
        s.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:60.0) Gecko/20100101 Firefox/60.0'
        s.headers['Cookie'] = "XSRF-TOKEN=" + header_cookie['XSRF-TOKEN']
        s.headers['Referer'] = LOGIN_URL
        s.headers['DNT'] = '1'
        s.headers['host'] = 'optemis.sentinel-asia.org'
        # Actual logging in
        r_signin = s.post(SIGNIN_URL, data=payload)
        r_signin.encoding = 'utf-8'
        print("Login status code:  {}".format(r_signin.status_code))
        print("Response text: {}".format(r_signin.text))
        print("Headers text: {}".format(s.headers))
        print("Payload text: {}".format(payload))
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


