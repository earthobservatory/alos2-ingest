#!/usr/bin/env python3
"""
Ingest ALOS2 data from a source to a destination:

  1) download data from a source and verify,
  2) extracts data and creates metadata
  3) push data to repository

"""

import os, re, requests, json, logging, traceback, argparse, shutil, glob
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
# import boto
import numpy as np
import scipy.spatial
from osgeo import gdal, osr
import alos2_utils
from subprocess import check_call

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

def checkProjectionWGS84(file):
    # check if file is suited for KML (needs to be projected in WGS 84 / EPSG 4326
    ds = gdal.Open(file)
    prj = ds.GetProjection()
    srs = osr.SpatialReference(wkt=prj)
    if "WGS 84" not in srs.GetAttrValue('geogcs'):
        vrt_file = "{}.vrt".format(os.path.splitext(file)[0])
        check_call("gdalwarp -of VRT -t_srs EPSG:4326 -overwrite {} {}".format(file, vrt_file), shell=True)
        ret_file = vrt_file
    else:
        ret_file = file

    return ret_file

def writeMask(out_file, arr, basefile):
    rows = arr.shape[0]
    cols = arr.shape[1]
    # create the raster file
    base_ds = gdal.Open(basefile)
    srs = osr.SpatialReference()  # establish encoding
    srs.ImportFromWkt(base_ds.GetProjectionRef())  # WGS84 lat/long
    if arr.ndim == 2:
        if arr.dtype == np.bool:
            # only 1 band, logical
            dst_ds = gdal.GetDriverByName('GTiff').Create(out_file, cols, rows, 1, gdal.GDT_Byte, options = ["NBITS=1", "COMPRESS=PACKBITS"])
            band1  = dst_ds.GetRasterBand(1)
            band1.WriteArray(arr)  # write band to the raster

        else:
            dst_ds = gdal.GetDriverByName('GTiff').Create(out_file, cols, rows, 1, gdal.GDT_Float32)
            dst_ds.GetRasterBand(1).WriteArray(arr)
            dst_ds.GetRasterBand(1).SetNoDataValue(0)

        dst_ds.SetGeoTransform(base_ds.GetGeoTransform())  # specify coords
        dst_ds.SetProjection(srs.ExportToWkt())  # export coords to file
        dst_ds.FlushCache()  # write to disk


def getFootprintJson(tif_file):
    tif_file = checkProjectionWGS84(tif_file)
    tmp_msk_file = os.path.join(os.path.dirname(os.path.abspath(tif_file)),"tmp_msk.tif")
    tmp_msk_nodata_file = os.path.join(os.path.dirname(os.path.abspath(tif_file)),"tmp_msk_nodata.tif")
    tmp_geojson = os.path.join(os.path.dirname(os.path.abspath(tif_file)),"tmp.json")
    tmp_final_geojson = os.path.join(os.path.dirname(os.path.abspath(tif_file)),"tmp_final.json")

    logging.info('Getting footprint of %s ...' % tif_file)
    ds = gdal.Open(tif_file)
    file_arr = np.array(ds.GetRasterBand(1).ReadAsArray())  # assuming we only care about the base layer
    footprint = file_arr != 0  # create radar footprint mask
    writeMask(tmp_msk_file, footprint, tif_file)
    logging.info("Creating polygon for file: %s" % tmp_msk_file)
    check_call("gdal_translate -a_nodata 0 {} {}".format(tmp_msk_file, tmp_msk_nodata_file), shell=True)
    check_call("gdal_polygonize.py -f GeoJSON {} {}".format(tmp_msk_nodata_file, tmp_geojson), shell=True)
    check_call("ogr2ogr -f GeoJSON -simplify 0.001 {} {}".format(tmp_final_geojson, tmp_geojson), shell=True)

    with open(tmp_final_geojson) as data_file:
        data = json.load(data_file)

    for tmpfile in glob.glob(os.path.join(os.path.dirname(os.path.abspath(tif_file)),'tmp*')):
        os.remove(tmpfile)

    return data['features'][0]['geometry']['coordinates'][0]

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


def productize(dataset_name, raw_dir, download_source):
    """Use extracted data to create metadata and the ALOS2 L1.1/L1.5/L2.1 product"""
    metadata, dataset, proddir = alos2_utils.create_product_base(raw_dir, dataset_name)

    # zipfile name for main product for posting to ARIA's dataset product directory
    archive_filename = os.path.join(proddir, "{}.zip".format(proddir))
    raw_dir_zipped = "{}.zip".format(raw_dir)
    # checks if raw-dir has a zip file equivalent, raw_dir_zipped
    if os.path.isfile(raw_dir_zipped):
        logging.info("Zipfile of raw_dir found. Moving %s to %s" % (raw_dir_zipped, archive_filename))
        shutil.move(raw_dir_zipped, archive_filename)
    else:
        logging.info("Zipfile of raw_dir not found. Repackaging contents of %s to %s" % (raw_dir, archive_filename))
        shutil.make_archive(os.path.splitext(archive_filename)[0], 'zip', raw_dir)

    if alos2_utils.ALOS2_L11 in dataset_name:
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
        # need_swath_poly = "2.1" in dataset_name
        tile_output_dir = "{}/tiles/".format(proddir)

        for tf in tiff_files:
            tif_file_path = os.path.join(raw_dir, tf)
            # process the geotiff to remove nodata
            processed_tif_disp = process_geotiff_disp(tif_file_path)

            # create the layer for facet view (only one layer created)
            if not os.path.isdir(tile_output_dir):
                # TODO: are tiles necessary?
                tile_max_zoom = 8
                layer = tiff_regex.match(tf).group(1)
                create_tiled_layer(os.path.join(tile_output_dir, layer), processed_tif_disp, zoom=[0, tile_max_zoom])
                tile_md["tile_layers"].append(layer)
                tile_md["tile_max_zoom"].append(tile_max_zoom)

            # create the browse pngs
            create_product_browse(processed_tif_disp)

            # create kmz
            # create_product_kmz(processed_tif_disp)

            # create swath polygon from gdal_polygonize,
            # UPDATE: 20191019, this does not work well in mountaineous regions! We saw this when ingesting data for Typhoon Hagibis: There were many polygons detected instead of 1.
            # if need_swath_poly:
            #     coordinates = getFootprintJson(processed_tif_disp)
            #     # Override cooirdinates from summary.txt
            #     metadata['location']['coordinates'] = [coordinates]
            #     dataset['location']['coordinates'] = [coordinates]
            #     # do this once only
            #     need_swath_poly = False

        # udpate the tiles
        metadata.update(tile_md)

    # move browsefile to proddir
    browse_files = sorted(glob.glob(os.path.join(raw_dir, '*browse*.png')))
    for fn in browse_files:
        shutil.move(fn, proddir)

    metadata["archive_filename"] = os.path.basename(archive_filename)
    metadata['download_source'] = download_source

    return metadata, dataset, proddir

def ingest_alos2(download_source):
    """Download file, push to repo and submit job for extraction."""

    pri_zip_paths = glob.glob('*.zip')
    # sec_zip_files = []
    for pri_zip_path in pri_zip_paths:
        alos2_utils.extract_nested_zip(pri_zip_path)

    raw_dir_list = []
    for root, subFolders, files in os.walk(os.getcwd()):
        if files:
            for x in files:
                m = re.search("IMG-[A-Z]{2}-ALOS2.{05}(.{04}-\d{6})-.{4}.*", x)
                if m:
                    logging.info("We found a ALOS2 dataset directory in: %s, adding to list" % root)
                    raw_dir_list.append(root)
                    break

    for raw_dir in raw_dir_list:
        dataset_name = alos2_utils.extract_dataset_name(raw_dir)
        # productize our extracted data
        metadata, dataset, proddir = productize(dataset_name, raw_dir, download_source)

        # dump metadata
        with open(os.path.join(proddir, dataset_name + ".met.json"), "w") as f:
            json.dump(metadata, f, indent=2)
            f.close()

        # dump dataset
        with open(os.path.join(proddir, dataset_name + ".dataset.json"), "w") as f:
            json.dump(dataset, f, indent=2)
            f.close()

        # cleanup raw_dir
        shutil.rmtree(raw_dir, ignore_errors=True)

    # cleanup downloaded zips in cwd
    for file in glob.glob('*.zip'):
        os.remove(file)

def load_context():
    with open('_context.json') as data_file:
        data = json.load(data_file)
        return data