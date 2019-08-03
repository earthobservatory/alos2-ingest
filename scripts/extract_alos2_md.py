#!/usr/bin/env python3
import glob
import os
from subprocess import check_call, check_output
import pickle
import isce
import argparse
from contrib.frameUtils.FrameInfoExtractor import FrameInfoExtractor
import datetime
import json
import re


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



def create_alos2_md_json(insar_obj, filename):
    FIE = FrameInfoExtractor()
    masterInfo = FIE.extractInfoFromFrame(insar_obj.frame)
    md = {}
    bbox  = masterInfo.bbox
    md['bbox_seq'] = ["nearEarlyCorner","farEarlyCorner", "nearLateCorner","farLateCorner"]
    md['bbox'] =  bbox
    md['geojson_poly'] = [[
        [bbox[0][1],bbox[0][0]], # nearEarlyCorner
        [bbox[1][1],bbox[1][0]], # farEarlyCorner
        [bbox[3][1],bbox[3][0]], # farLateCorner
        [bbox[2][1],bbox[2][0]], # nearLateCorner
        [bbox[0][1],bbox[0][0]], # nearEarlyCorner
    ]]
    md['sensingStart'] = masterInfo.sensingStart.strftime("%Y-%m-%dT%H:%M:%S.%f")
    md['sensingStop'] = masterInfo.sensingStop.strftime("%Y-%m-%dT%H:%M:%S.%f")
    md['orbitNumber'] = masterInfo.orbitNumber
    md['spacecraftName'] = masterInfo.spacecraftName
    md['frameNumber'] = masterInfo.frameNumber
    md['direction'] = masterInfo.direction
    md['squintAngle'] = insar_obj.frame.squintAngle

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
    return parser.parse_args()

if __name__ == '__main__':
    args = cmdLineParse()
    insar_obj = get_alos2_obj(args.alos2dir)
    create_alos2_md_json(insar_obj, args.op_json)






