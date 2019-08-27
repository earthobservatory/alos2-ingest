#!/usr/bin/env python
"""
Ingest ALOS2 data from a source to a destination:

  1) download data from a source and verify,
  2) extracts data and creates metadata
  3) push data to repository


HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.
"""

import os, re, requests, json, logging, traceback, argparse, shutil, glob
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
# import boto
import numpy as np
import scipy.spatial
from osgeo import gdal
import alos2_utils
from subprocess import check_call
import scripts.auig2_download as auig2

# disable warnings for SSL verification
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)



# scale range
SCALE_RANGE=[0, 7500]

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
    """Get L2.1 actual polygon coordinates with convex hull"""
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
    """Reprocess JAXA's L1./ L2.1 geotiff to include nodata = 0 for display"""
    # removes nodata value from original geotiff file from jaxa
    outfile = os.path.splitext(infile)[0] + "_disp.tif"
    logging.info("Removing nodata and scaling intensity from %s to %s. Scale intensity at %s"
                 % (infile, outfile, SCALE_RANGE))
    options_string = '-of GTiff -ot Byte -scale {} {} 0 255 -a_nodata 0'.format(SCALE_RANGE[0], SCALE_RANGE[1])
    gdal_translate(outfile, infile, options_string)
    return outfile


def create_tiled_layer(output_dir, tiff_file, zoom=[0, 8]):
    """Use extracted data to create tiles for display on tosca"""
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
    """Use extracted data to create browse images for display on tosca"""
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
    """Use extracted data to create metadata and the ALOS2 L1.1/L1.5/L2.1 product"""
    is_l11 = alos2_utils.ALOS2_L11 in dataset_name
    metadata, dataset, proddir = alos2_utils.create_product_base(raw_dir, dataset_name, is_l11)

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

def ingest_alos2(download_source, file_type, path_number=None):
    """Download file, push to repo and submit job for extraction."""

    pri_zip_paths = glob.glob('*.zip')
    sec_zip_files = []
    for pri_zip_path in pri_zip_paths:
        # verify downloaded file was not corrupted
        logging.info("Verifying %s is file type %s." % (pri_zip_path, file_type))
        try:
            sec_zip_dir = alos2_utils.verify_and_extract(pri_zip_path, file_type)
            logging.info("seec zip dir: %s" % sec_zip_dir)

            # unzip the second layer to gather metadata
            sec_zip_files = glob.glob(os.path.join(sec_zip_dir,'*.zip'))
            logging.info("glob dir: %s" % os.path.join(sec_zip_dir,'*.zip'))

            if not len(sec_zip_files) > 0:
                raise RuntimeError("Unable to find second zipfiles under %s" % sec_zip_dir)

        except Exception as e:
            tb = traceback.format_exc()
            logging.error("Failed to verify and extract files of type %s: %s" % \
                          (file_type, tb))
            raise

    for sec_zip_file in sec_zip_files:
        logging.info("Verifying %s is file type %s." % (sec_zip_file, file_type))
        raw_dir = alos2_utils.verify_and_extract(sec_zip_file, file_type)

        # get the datasetname from IMG files in raw_dir
        dataset_name = alos2_utils.extract_dataset_name(raw_dir)

        # productize our extracted data
        metadata, dataset, proddir = productize(dataset_name, raw_dir, sec_zip_file, download_source)

        #checks path number formulation:
        alos2_utils.check_path_num(metadata, path_number)

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
                        choices=alos2_utils.ALL_TYPES, required=False)
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
            alos2_utils.download(args.download_url, args.oauth_url)
            download_source = args.download_url
        elif args.order_id:
            # auig2.download(args)
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
