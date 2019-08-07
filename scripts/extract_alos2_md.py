#!/usr/bin/env python3
import glob
import os
from subprocess import check_call, check_output
import pickle
import argparse
import datetime
import json
import re
import requests

def create_insar_xml(scene_xml):
    fp = open('insarApp.xml', 'w')
    fp.write('<insarApp>\n')
    fp.write('<component name="insar">\n')
    fp.write('        <property  name="Sensor name">ALOS2</property>\n')
    fp.write('        <property name="dopplermethod">useDEFAULT</property>\n')
    fp.write('        <component name="master">\n')
    fp.write('            <catalog>{}</catalog>\n'.format(scene_xml))
    fp.write('        </component>\n')
    fp.write('        <component name="slave">\n')
    fp.write('            <catalog>{}</catalog>\n'.format(scene_xml))
    fp.write('        </component>\n')
    fp.write('    </component>\n')
    fp.write('</insarApp>\n')
    fp.close()


def create_scene_xml(led_filename,img_filename):
    scenefile = 'scene.xml'
    fp = open(scenefile, 'w')
    fp.write('<component>\n')
    fp.write('    <property name="IMAGEFILE">\n')
    fp.write('        <value>{}</value>\n'.format(img_filename))
    fp.write('    </property>\n')
    fp.write('    <property name="LEADERFILE">\n')
    fp.write('        <value>{}</value>\n'.format(led_filename))
    fp.write('    </property>\n')
    fp.write('    <property name="OUTPUT">\n')
    fp.write('        <value>dummy.raw</value>\n')
    fp.write('    </property>\n')
    fp.write('</component>\n')
    fp.close()
    return scenefile


def get_alos2_obj(dir_name):
    import isce
    insar_obj = None
    dataset_name = None
    led_file = sorted(glob.glob(os.path.join(dir_name, 'LED*')))
    img_file = sorted(glob.glob(os.path.join(dir_name, 'IMG*')))

    if len(img_file) > 0 and len(led_file)>0:
        scenefile = create_scene_xml(led_file[0], img_file[0])
        create_insar_xml(scenefile)
        check_output("insarApp.py --steps --end=preprocess", shell=True)
        f = open("PICKLE/preprocess", "rb")
        insar_obj = pickle.load(f)

    return insar_obj



def create_alos2_md_isce(insar_obj, filename):
    from contrib.frameUtils.FrameInfoExtractor import FrameInfoExtractor
    FIE = FrameInfoExtractor()
    masterInfo = FIE.extractInfoFromFrame(insar_obj.frame)
    md = {}
    bbox  = masterInfo.bbox
    md['bbox_seq'] = ["nearEarlyCorner","farEarlyCorner", "nearLateCorner","farLateCorner"]
    md['bbox'] =  bbox
    md['geometry'] = {
        "coordinates":[[
        [bbox[0][1],bbox[0][0]], # nearEarlyCorner
        [bbox[1][1],bbox[1][0]], # farEarlyCorner
        [bbox[3][1],bbox[3][0]], # farLateCorner
        [bbox[2][1],bbox[2][0]], # nearLateCorner
        [bbox[0][1],bbox[0][0]], # nearEarlyCorner
        ]],
        "type":"Polygon"
    }
    md['start_time'] = masterInfo.sensingStart.strftime("%Y-%m-%dT%H:%M:%S.%f")
    md['stop_time'] = masterInfo.sensingStop.strftime("%Y-%m-%dT%H:%M:%S.%f")
    md['absolute_orbit'] = masterInfo.orbitNumber
    md['frame'] = masterInfo.frameNumber
    md['flight_direction'] = masterInfo.direction
    md['satellite_name'] = masterInfo.spacecraftName
    md['source'] = "isce_preprocessing"

    with open(filename, "w") as f:
        json.dump(md, f, indent=2)
        f.close()

def create_alos2_md_bos(dir_name, filename):
    img_file = sorted(glob.glob(os.path.join(dir_name, 'IMG*')))
    geo_server = "https://portal.bostechnologies.com/geoserver/bos/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=bos:sarcat&maxFeatures=50&outputFormat=json"
    if len(img_file) > 0:
        m = re.search('IMG-[A-Z]{2}-(ALOS2.{16})-.*', os.path.basename(img_file[0]))
        id = m.group(1)
        params = {'cql_filter': "(identifier='{}')".format(id)}

        r = requests.get(geo_server, params)
        r.raise_for_status()

        md = r.json()["features"][0]
        md['source'] = "bos_sarcat"
        # move properties a level up
        md.update(md['properties'])
        del md['properties']
        with open(filename, "w") as f:
            json.dump(md, f, indent=2)
            f.close()

def cmdLineParse():
    '''
    Command line parser.
    '''
    parser = argparse.ArgumentParser( description='extract metadata from ALOS2 1.1 with ISCE')
    parser.add_argument('--dir', dest='alos2dir', type=str, default=".",
            help = 'directory containing the L1.1 ALOS2 CEOS files')
    parser.add_argument('--output', dest='op_json', type=str, default="alos2_md.json",
                        help='json file name to output metadata to')
    parser.add_argument('--method', dest='method', type=str, default="",
                        help='either "bos" (to get md from bos) or "isce" (to get md from isce preprocessing) or empty (to get from bos, fallback isce)')
    return parser.parse_args()

if __name__ == '__main__':
    args = cmdLineParse()
    if args.method == "bos":
        create_alos2_md_bos(args.alos2dir, args.op_json)
    elif args.method == "isce":
        insar_obj = get_alos2_obj(args.alos2dir)
        create_alos2_md_isce(insar_obj, args.op_json)
    else:
        try:
            create_alos2_md_bos(args.alos2dir, args.op_json)
        except Exception as e:
            print("Got exception trying to query bos sarcat: %s" % str(e))
            # use isce if we are unable to get the bbox from bos
            insar_obj = get_alos2_obj(args.alos2dir)
            create_alos2_md_isce(insar_obj, args.op_json)






