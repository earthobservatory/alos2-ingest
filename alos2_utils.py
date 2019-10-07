#!/usr/bin/env python3

import re
import zipfile
import configparser
import io
import datetime, os, json, logging, traceback
from subprocess import check_call, check_output
import glob
ALOS2_L11 = "1.1"


def download(download_url):
    # download
    dest = os.path.basename(download_url)
    logging.info("Downloading %s to %s." % (download_url, dest))
    try:
        cmd = "python -c \'import osaka.main; osaka.main.get(\"%s\", \"%s\", params={\"oauth\": None}, measure=True, output=\"./pge_metrics.json\")\'" % (download_url, dest)
        logging.info(cmd)
        check_output(cmd, shell=True)
    except Exception as e:
        tb = traceback.format_exc()
        logging.error("Failed to download %s to %s: %s" % (download_url,
                                                           dest, tb))
        raise

def verify_and_extract(zip_file):
    """Verify downloaded file is okay by checking that it can
       be unzipped/untarred."""
    unzip_dir = None
    if not zipfile.is_zipfile(zip_file):
        raise RuntimeError("%s is not a zipfile." % zip_file)
    with zipfile.ZipFile(zip_file, 'r') as f:
        ret = f.testzip()
        if ret:
            raise RuntimeError("%s is corrupt. Test zip returns: %s" % (zip_file, ret))
        else:
            unzip_dir = os.path.abspath(zip_file.replace(".zip", ""))
            f.extractall(unzip_dir)
    return unzip_dir

def extract_nested_zip(zippedFile):
    """ Extract a zip file including any nested zip files
        Delete the zip file(s) after extraction
    """
    logging.info("extracting %s"  % zippedFile)
    unzip_dir = verify_and_extract(zippedFile)
    logging.info("walking through %s"  % unzip_dir)
    for root, dirs, files in os.walk(unzip_dir):
        for filename in files:
            if re.search(r'\.zip$', filename):
                fileSpec = os.path.join(root, filename)
                logging.info("submitting zip file extraction %s"  % fileSpec)
                extract_nested_zip(fileSpec)


def md_frm_dataset_name(metadata, dataset_name):
    metadata['prod_name'] = dataset_name
    metadata['spacecraftName'] = dataset_name[0:5]
    metadata['platform'] = metadata['spacecraftName']
    metadata['dataset_type'] = dataset_name[0:5]
    metadata['orbitNumber'] = int(dataset_name[5:10])
    metadata['frameID'] = int(dataset_name[10:14])
    # this emprical forumla to get path/track number is derived from Eric Lindsey's modeling and fits for all L1.1 data
    metadata['trackNumber'] = int((14 * metadata['orbitNumber'] + 24) % 207)
    prod_datetime = datetime.datetime.strptime(dataset_name[15:21], '%y%m%d')
    prod_date = prod_datetime.strftime("%Y-%m-%d")
    metadata['prod_date'] = prod_date

    # TODO: not sure if this is the right way to expose this in Facet Filters, using CSK's metadata structure
    dfdn = {"AcquistionMode": dataset_name[22:25],
            "LookSide": dataset_name[25]}
    metadata['dfdn'] = dfdn

    metadata['lookDirection'] = "right" if dataset_name[25] is "R" else "left"
    metadata['level'] = "L" + dataset_name[26:29]
    metadata['processingOption'] = dataset_name[29]
    metadata['mapProjection'] = dataset_name[30]
    metadata['direction'] = "ascending" if dataset_name[31] is "A" else "descending"

    return metadata


def md_frm_summary(summary_file, metadata):
    # open summary.txt to extract metadata
    # extract information from summary see: https://www.eorc.jaxa.jp/ALOS-2/en/doc/fdata/PALSAR-2_xx_Format_GeoTIFF_E_r.pdf
    logging.info("Extracting metadata from %s" % summary_file)
    dummy_section = "summary"
    with open(summary_file, 'r') as f:
        # need to add dummy section for config parse to read .properties file
        summary_string = '[%s]\n' % dummy_section + f.read()
    summary_string = summary_string.replace('"', '')
    buf = io.StringIO(summary_string)
    config = configparser.ConfigParser()
    config.readfp(buf)

    # parse the metadata from summary.txt
    alos2md = {}
    for name, value in config.items(dummy_section):
        alos2md[name] = value

    metadata['alos2md'] = alos2md

    # others
    metadata['dataset'] = "ALOS2_GeoTIFF" if "TIFF" in metadata['alos2md']['pdi_productformat'] else "ALOS2_CEOS"
    metadata['source'] = "jaxa"

    location = {}
    location['type'] = 'Polygon'
    location['coordinates'] = [[
        [float(alos2md['img_imagescenelefttoplongitude']), float(alos2md['img_imagescenelefttoplatitude'])],
        [float(alos2md['img_imagescenerighttoplongitude']), float(alos2md['img_imagescenerighttoplatitude'])],
        [float(alos2md['img_imagescenerightbottomlongitude']), float(alos2md['img_imagescenerightbottomlatitude'])],
        [float(alos2md['img_imagesceneleftbottomlongitude']), float(alos2md['img_imagesceneleftbottomlatitude'])],
        [float(alos2md['img_imagescenelefttoplongitude']), float(alos2md['img_imagescenelefttoplatitude'])]
        ]]
    metadata['location'] = location
    metadata['starttime'] = datetime.datetime.strptime(alos2md['img_scenestartdatetime'], '%Y%m%d %H:%M:%S.%f').strftime("%Y-%m-%dT%H:%M:%S.%f")
    metadata['endtime'] = datetime.datetime.strptime(alos2md['img_sceneenddatetime'], '%Y%m%d %H:%M:%S.%f').strftime("%Y-%m-%dT%H:%M:%S.%f")
    return metadata

def md_frm_extractor(alos2_dir, metadata):

    # extract metadata with isce from IMG files
    md_file = "alos2_md.json"
    cmd = "{}/scripts/extract_alos2_md.py --dir {} --output {}".format(
        os.path.dirname(os.path.realpath(__file__)),
        alos2_dir,
        md_file)

    check_call(cmd, shell=True)
    md = json.load(open(md_file))

    metadata['alos2md'] = md
    metadata['dataset'] = "ALOS2-L1.1_SLC"
    metadata['source'] = "jaxa"
    metadata['location'] = md['geometry']
    metadata['starttime'] = md['start_time']
    metadata['endtime'] = md['stop_time']

    return metadata


def create_metadata(alos2_dir, dataset_name, is_l11):
    metadata = {}
    metadata = md_frm_dataset_name(metadata, dataset_name)
    summary_file = os.path.join(alos2_dir, "summary.txt")
    if os.path.exists(summary_file) and not is_l11:
        metadata = md_frm_summary(summary_file, metadata)
    elif is_l11:
        metadata = md_frm_extractor(alos2_dir, metadata)
    else:
        raise RuntimeError("Cannot recognise ALOS2 directory format!")

    return metadata


def create_dataset(metadata, is_l11):
    logging.info("Extracting datasets from metadata")
    # get settings for dataset version
    settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'settings.json')

    settings = json.load(open(settings_file))

    # datasets.json
    # extract metadata for datasets
    version = settings['ALOS2_SLC_VERSION'] if is_l11 else settings['ALOS2_GEOTIFF_VERSION']
    dataset = {
        'version': version,
        'label': metadata['prod_name'],
        'starttime': metadata['starttime'],
        'endtime': metadata['endtime']
    }
    dataset['location'] = metadata['location']

    return dataset

def create_product_base(raw_dir, dataset_name, is_l11):
    metadata = create_metadata(raw_dir, dataset_name, is_l11)

    # create dataset.json
    dataset = create_dataset(metadata, is_l11)

    # create the product directory
    proddir = os.path.join(".", dataset_name)
    if not os.path.exists(proddir):
        os.makedirs(proddir)

    return metadata, dataset, proddir

def extract_dataset_name(raw_dir):
    # figure out dataset name to start creating metadata
    img_file = sorted(glob.glob(os.path.join(raw_dir, 'IMG*')))
    dataset_name = None
    if len(img_file) > 0:
        m = re.search('IMG-[A-Z]{2}-(ALOS2.{27}).*', os.path.basename(img_file[0]))
        if m:
            dataset_name = m.group(1)

    else:
        raise RuntimeError("Unable to find any ALOS2 image files to process!")

    return dataset_name

# def check_path_num(metadata, path_number):
#     if path_number:
#         path_num = int(float(path_number))
#         logging.info("Checking manual input path number {} against formulated path number {}"
#                      .format(path_num, metadata['trackNumber']))
#         if path_num != metadata['trackNumber']:
#             raise RuntimeError("There might be an error in the formulation of path number. "
#                                "Formulated path_number: {} | Manual input path_number: {}"
#                                .format(metadata['trackNumber'], path_num))

