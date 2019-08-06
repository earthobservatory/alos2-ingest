#!/usr/bin/env python
"""
Ingest ALOS2 data from a source to a destination:

  1) download data from a source and verify,
  2) extracts data and creates metadata
  3) push data to repository


HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.
"""

import datetime, os, sys, re, requests, json, logging, traceback, argparse, shutil, glob
import zipfile
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
# import boto
import osaka.main
import numpy as np
import scipy.spatial
from osgeo import gdal

import ConfigParser
import StringIO
#
# from hysds.orchestrator import submit_job
# import hysds.orchestrator
# from hysds.celery import app
# from hysds.dataset_ingest import ingest
# from hysds_commons.job_rest_utils import single_process_and_submission
from subprocess import check_call
import scripts.auig2_download as auig2

# disable warnings for SSL verification
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# all file types
ALL_TYPES = []

# zip types
ZIP_TYPE = ["zip"]
ALL_TYPES.extend(ZIP_TYPE)

# scale range
SCALE_RANGE=[0, 7500]


def verify_and_extract(zip_file, file_type):
    """Verify downloaded file is okay by checking that it can
       be unzipped/untarred."""
    prod_dir = None
    if file_type in ZIP_TYPE:
        if not zipfile.is_zipfile(zip_file):
            raise RuntimeError("%s is not a zipfile." % zip_file)
        with zipfile.ZipFile(zip_file, 'r') as f:
            ret = f.testzip()
            if ret:
                raise RuntimeError("%s is corrupt. Test zip returns: %s" % (zip_file, ret))
            else:
                prod_dir = zip_file.replace(".zip", "")
                f.extractall(prod_dir)

    else:
        raise NotImplementedError("Failed to verify %s is file type %s." % \
                                  (zip_file, file_type))

    return prod_dir


def download(download_url, oauth_url):
    # download
    dest = os.path.basename(download_url)
    logging.info("Downloading %s to %s." % (download_url, dest))
    try:
        osaka.main.get(download_url, dest, params={"oauth": oauth_url}, measure=True, output="./pge_metrics.json")
    except Exception as e:
        tb = traceback.format_exc()
        logging.error("Failed to download %s to %s: %s" % (download_url,
                                                           dest, tb))
        raise

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
    buf = StringIO.StringIO(summary_string)
    config = ConfigParser.ConfigParser()
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


def create_metadata(alos2_dir, dataset_name):
    # TODO: Some of these are hardcoded! Do we need them?
    metadata = {}
    metadata = md_frm_dataset_name(metadata, dataset_name)
    summary_file = os.path.join(alos2_dir, "summary.txt")
    if os.path.exists(summary_file) and not "1.1" in dataset_name:
        metadata = md_frm_summary(summary_file, metadata)
    elif "1.1" in dataset_name:
        metadata = md_frm_extractor(alos2_dir, metadata)
    else:
        raise RuntimeError("Cannot recognise ALOS2 directory format!")

    return metadata


def create_dataset(metadata):
    logging.info("Extracting datasets from metadata")
    # get settings for dataset version
    settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'settings.json')

    settings = json.load(open(settings_file))

    # datasets.json
    # extract metadata for datasets
    dataset = {
        'version': settings['ALOS2_INGEST_VERSION'],
        'label': metadata['prod_name'],
        'starttime': metadata['starttime'],
        'endtime': metadata['endtime']
    }
    dataset['location'] = metadata['location']

    return dataset


def gdal_translate(outfile, infile, options_string):
    cmd = "gdal_translate {} {} {}".format(options_string, infile, outfile)
    logging.info("cmd: %s" % cmd)
    return check_call(cmd,  shell=True)

def get_bounding_polygon(vrt_file):
    '''
    Get the minimum bounding region
    @param path - path to h5 file from which to read TS data
    '''
    ds = gdal.Open(vrt_file)
    #Read out the first data frame, lats vector and lons vector.
    data = np.array(ds.GetRasterBand(1).ReadAsArray())
    logging.info("Array size of data {}".format(data.shape))
    lats, lons = get_geocoded_coords(vrt_file)
    logging.info("Array size of lats {}".format(lats.shape))
    logging.info("Array size of lons {}".format(lons.shape))

    #Create a grid of lon, lat pairs
    coords = np.dstack(np.meshgrid(lons,lats))
    #Calculate any point in the data that is not 0, and grab the coordinates
    inx = np.nonzero(data)
    points = coords[inx]
    #Calculate the convex-hull of the data points.  This will be a mimimum
    #bounding convex-polygon.
    hull = scipy.spatial.ConvexHull(points)
    #Harvest the points and make it a loop
    pts = [list(pt) for pt in hull.points[hull.vertices]]
    logging.info("Number of vertices: {}".format(len(pts)))
    pts.append(pts[0])
    return pts


def get_geocoded_coords(vrt_file):
    """Return geocoded coordinates of radar pixels."""

    # extract geo-coded corner coordinates
    ds = gdal.Open(vrt_file)
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    lon_arr = list(range(0, cols))
    lat_arr = list(range(0, rows))
    lons = np.empty((cols,))
    lats = np.empty((rows,))
    for py in lat_arr:
        lats[py] = gt[3] + (py * gt[5])
    for px in lon_arr:
        lons[px] = gt[0] + (px * gt[1])
    return lats, lons

# ONLY FOR L2.1 which is Geo-coded (Map projection based on north-oriented map direction)
def get_swath_polygon_coords(processed_tif):
    # create vrt file with wgs84 coordinates
    file_basename = os.path.splitext(processed_tif)[0]
    cmd = "gdalwarp -dstnodata 0 -dstalpha -of vrt -t_srs EPSG:4326 {} {}.vrt".format(processed_tif, file_basename)
    logging.info("cmd: %s" % cmd)
    check_call(cmd, shell=True)

    logging.info('Getting polygon of satellite footprint swath.')
    polygon_coords = get_bounding_polygon("{}.vrt".format(file_basename))
    logging.info("Coordinates of subswath polygon: {}".format(polygon_coords))

    return polygon_coords

def process_geotiff_disp(infile):
    # removes nodata value from original geotiff file from jaxa
    outfile = os.path.splitext(infile)[0] + "_disp.tif"
    logging.info("Removing nodata and scaling intensity from %s to %s. Scale intensity at %s"
                 % (infile, outfile, SCALE_RANGE))
    options_string = '-of GTiff -ot Byte -scale {} {} 0 255 -a_nodata 0'.format(SCALE_RANGE[0], SCALE_RANGE[1])
    gdal_translate(outfile, infile, options_string)
    return outfile


def create_tiled_layer(output_dir, tiff_file, zoom=[0, 8]):
    # create tiles from geotiff for facetView dispaly
    logging.info("Generating tiles.")
    zoom_i = zoom[0]
    zoom_f = zoom[1]

    while zoom_f > zoom_i:
        try:
            cmd = "gdal2tiles.py -z {}-{} -p mercator -a 0,0,0 {} {}".format(zoom_i, zoom_f, tiff_file, output_dir)
            logging.info("cmd: %s" % cmd)
            check_call(cmd, shell=True)
            break
        except Exception as e:
            logging.warn("Got exception running {}: {}".format(cmd, str(e)))
            logging.warn("Traceback: {}".format(traceback.format_exc()))
            zoom_f -= 1


def create_product_browse(file):
    options_string = '-of PNG'
    if "tif" in file:
        # tiff files are huge, our options need to resize them
        options_string += ' -outsize 10% 10%'
    elif "WBD" in file:
        # scansar L1.1 images have 1:7 aspect ratio
        options_string += ' -outsize 100% 40%'

    logging.info("Creating browse png from %s" % file)
    out_file = os.path.splitext(file)[0] + '.browse.png'
    out_file_small = os.path.splitext(file)[0] + '.browse_small.png'
    gdal_translate(out_file, file, options_string)
    os.system("convert -resize 250x250 %s %s" % (out_file, out_file_small))
    return

def productize(dataset_name, raw_dir, zip_file, download_source):
    metadata = create_metadata(raw_dir, dataset_name)
    is_l11 = "1.1" in dataset_name

    # create dataset.json
    dataset = create_dataset(metadata)

    # create the product directory
    proddir = os.path.join(".", dataset_name)
    if not os.path.exists(proddir):
        os.makedirs(proddir)

    if is_l11:
        # create browse only for L1.1 data (if available)
        jpg_files = sorted(glob.glob(os.path.join(raw_dir, '*.jpg')))

        for jpg in jpg_files:
            create_product_browse(jpg)

    else:
        # create post products (tiles) for L1.5 / L2.1 data
        tiff_regex = re.compile("IMG-([A-Z]{2})-ALOS2(.{27}).tif")
        tiff_files = [f for f in os.listdir(raw_dir) if tiff_regex.match(f)]

        tile_md = {"tiles": True, "tile_layers": [], "tile_max_zoom": []}

        # we need to override the coordinates bbox to cover actual swath if dataset is Level2.1
        # L2.1 is Geo-coded (Map projection based on north-oriented map direction)
        need_swath_poly = "2.1" in dataset_name
        tile_output_dir = "{}/tiles/".format(proddir)

        for tf in tiff_files:
            tif_file_path = os.path.join(raw_dir, tf)
            # process the geotiff to remove nodata
            processed_tif_disp = process_geotiff_disp(tif_file_path)

            # create the layer for facet view (only one layer created)
            if not os.path.isdir(tile_output_dir):
                tile_max_zoom = 12
                layer = tiff_regex.match(tf).group(1)
                create_tiled_layer(os.path.join(tile_output_dir, layer), processed_tif_disp, zoom=[0, tile_max_zoom])
                tile_md["tile_layers"].append(layer)
                tile_md["tile_max_zoom"].append(tile_max_zoom)

            # create the browse pngs
            create_product_browse(processed_tif_disp)

            # create_product_kmz(processed_tif_disp)

            if need_swath_poly:
                coordinates = get_swath_polygon_coords(processed_tif_disp)
                # Override cooirdinates from summary.txt
                metadata['location']['coordinates'] = [coordinates]
                dataset['location']['coordinates'] = [coordinates]
                # do this once only
                need_swath_poly = False

        # udpate the tiles
        metadata.update(tile_md)

    # move browsefile to proddir
    browse_files = sorted(glob.glob(os.path.join(raw_dir, '*browse*.png')))
    for fn in browse_files:
        shutil.move(fn, proddir)

    # move main zip file as dataset to proddir
    archive_filename = "{}/{}.zip".format(proddir, proddir)
    shutil.move(zip_file, archive_filename)
    metadata["archive_filename"] = os.path.basename(archive_filename)
    metadata['download_source'] = download_source

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

def check_path_num(metadata, path_number):
    if path_number:
        path_num = int(float(path_number))
        logging.info("Checking manual input path number {} against formulated path number {}"
                     .format(path_num, metadata['trackNumber']))
        if path_num != metadata['trackNumber']:
            raise RuntimeError("There might be an error in the formulation of path number. "
                               "Formulated path_number: {} | Manual input path_number: {}"
                               .format(metadata['trackNumber'], path_num))



def ingest_alos2(download_source, file_type, path_number=None):
    """Download file, push to repo and submit job for extraction."""

    pri_zip_paths = glob.glob('*.zip')

    for pri_zip_path in pri_zip_paths:
        # verify downloaded file was not corrupted
        logging.info("Verifying %s is file type %s." % (pri_zip_path, file_type))
        try:
            sec_zip_dir = verify_and_extract(pri_zip_path,file_type)

            # unzip the second layer to gather metadata
            sec_zip_file = glob.glob(os.path.join(sec_zip_dir,'*.zip'))
            if not len(sec_zip_file) == 1:
                raise RuntimeError("Unable to find second zipfile under %s" % sec_zip_dir)

            logging.info("Verifying %s is file type %s." % (sec_zip_file[0], file_type))
            raw_dir = verify_and_extract(sec_zip_file[0], file_type)

        except Exception as e:
            tb = traceback.format_exc()
            logging.error("Failed to verify and extract files of type %s: %s" % \
                          (file_type, tb))
            raise

        # get the datasetname from IMG files in raw_dir
        dataset_name = extract_dataset_name(raw_dir)

        # productize our extracted data
        metadata, dataset, proddir = productize(dataset_name, raw_dir, sec_zip_file[0], download_source)

        #checks path number formulation:
        check_path_num(metadata, path_number)

        # dump metadata
        with open(os.path.join(proddir, dataset_name + ".met.json"), "w") as f:
            json.dump(metadata, f, indent=2)
            f.close()

        # dump dataset
        with open(os.path.join(proddir, dataset_name + ".dataset.json"), "w") as f:
            json.dump(dataset, f, indent=2)
            f.close()

        #cleanup
        shutil.rmtree(sec_zip_dir, ignore_errors=True)
        # retaining primary zip, we can delete it if we want
        # os.remove(pri_zip_path)

def load_context():
    with open('_context.json') as data_file:
        data = json.load(data_file)
        return data

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
    parser.add_argument("--path_number_to_check", help="Path number provided from ALOS2 Ordering system to "
                                                     "check against empirical formulation.", required=False)
    parser.add_argument("--oauth_url", help="OAuth authentication URL " +
                                            "(credentials stored in " +
                                            ".netrc)", required=False)
    parser.add_argument("--file_type", dest='file_type', help="download file type to verify", default='zip',
                        choices=ALL_TYPES, required=False)
    return parser.parse_args()

if __name__ == "__main__":
    args = cmdLineParse()

    try:
        # first check if we need to read from _context.json
        if not (args.download_url or args.order_id):
            # no inputs defined (as per defaults)
            # we need to try to load from context
            ctx = load_context()
            args.download_url = ctx["download_url"]
            args.order_id  = ctx["auig2_orderid"]
            args.username=ctx["auig2_username"]
            args.password=ctx["auig2_password"]
            args.path_number_to_check=ctx["path_number_to_check"]

        if args.download_url:
            # download(args.download_url, args.oauth_url)
            download_source = args.download_url
        elif args.order_id:
            auig2.download(args)
            download_source = "UN:%s_OrderID:%s"
        else:
            raise RuntimeError("Unable to do anything. Download parameters not defined. "
                               "Input args: {}".format(str(args)))

        ingest_alos2(download_source, args.file_type, path_number=args.path_number_to_check)

    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
