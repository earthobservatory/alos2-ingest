# alos2-ingest
ARIA PGEs

## PGEs
- ALOS2 L1.5 and L2.1 ingestion (GeoTIFF format)
    - download
    - extract
    - create visualization with map tiles on leaflet
        * compute polygon from GTiff (zoom level 0-14)

- ALOS2 L1.1 ingestion (CEOS format)
    - download
    - extract
    - attempt to get the coverage area via BOS SARCAT, if scene_id not found --> uses ISCE ALOS-2 insarApp preprocessing step

Associated job:
### Job 1: ALOS2 Ingest from AUIG2
- Type: **Individual**
- Facet: None required
- User inputs:

    | Fields        | Description   | Type  |
    | ------------- |-------------| :---------:| 
    | `auig2_orderid`     | Order ID from AUIG2 portal | str |  
    | `auig2_username`      | Account username associated with this order    |  str | 
    | `auig2_password` | Account password associated with this order |  str | 


### Job 2: ALOS2 Ingest from Sentinel Asia
- Type: **Individual**
- Facet: None required
- User inputs:

    | Fields        | Description   | Type  | Example |
    | ------------- |-------------| :---------:| :-----|
    | `data_id`     | Data ID of singular ALOS-2 file from Sentinel-Asia to ingest  | str |  `JPJXisis0001201908160001` |
    | `eor_id`      | EOR ID of the event - will ingest all compatible ALOS-2 files from that EOR.    |  str |  `ERAHAC000007` |
    | `start_date` | Date to start scraping (YYYYMMDD) - will scrape for all EOR IDs from start_date to now   |  str |  `20190930` |
    | `queue` | Autoscaling queue to submit multiple downloads for. Only applicable for `eor_id` or `start_date`.  |  str |  `aria-job_worker-large` |
    
    _*Note: Only specify either `data_id` / `eor_id` / `start_date` for each job_

### Job 3: ALOS2 Ingest from Download URL
- Type: **Individual**
- Facet: None required
- User inputs:

    | Fields        | Description   | Type  | Example |
    | ------------- |-------------| :---------:| :-----|
    | `download_url`     | URL where an ALOS-2 zipped file stored to be downloaded. E.g. from a webdav server | str |  `https"//my-webdav-url/test/235320010.zip` |

## Ingesting ALOS2 L1.1 from gekko HPC
This package also has scripts to create ALOS-2 metadata from stored ALOS-2 data in a HPC system and ingesting it into the ARIA system to reflect the archive.
This workflow utilizes the HPC nodes to run the scripts that create the neccessary metadata and ingests it into the ARIA system. Hence, hysds libraries has to be installed in the HPC system for this to work.

Available scripts:
- `scrape_alos2_aria_ingest.py`
    - Given a directory, `-dir`, recursively walk through directories for any `IMG-HH*`/`IMG-HV*` files. Extract the unique scene date and submit a `ingest-alos2-md` job via `ingest2aria.pbs`(with folder scene is stored and date)
- `ingest_alos2_md.py`
    - Runs in HPC nodes
    - Given a directory `-dir`, and date `-fdate`, creates a temp directory, symlinking all files with `*fdate*` in `dir`. 
    - Gather file locations and create `metadata.json`, `dataset.json` based on `IMG-H*` filenames and bos-sarcat query. Ingest metadata into ARIA.

- `ingest2aria.pbs`
   - PBS HPC script to submit `ingest_alos2_md` to nodes.

####To ingest metadata for ALOS-2 stored in NTU gekko:

```bash
python3 ./alos2-ingest/gekko/scrape_alos2_aria_ingest.py -dir /home/data/INSAR/ALOS2/ -pbs ./alos2-ingest/gekko/ingest2aria.pbs 

```
## ALOS2 Datasets
- Based on the `IMG-*` file names in the uncompressed ALOS-2 package, the above jobs will all create different ALOS-2 datasets (L1.1/L1.5/L2.1):

    | ALOS-2 Format    | `type`   | `dataset`  |
    | ------------- |-------------| :-----|
    | ALOS L1.1 CEOS   | `alos2_slc`  | `ALOS2-L1.1-SLC` |
    | ALOS L1.5 GeoTIFF | `L1.5_geotiff`  |  `ALOS2_GeoTIFF`  |
    | ALOS L2.1 GeoTIFF | `L2.1_geotiff`  |  `ALOS2_GeoTIFF`  |

- Product format:

    | Product        | Description   | Example  |
    | ------------- |-------------| :-----|
    | ALOS2 Main Product   | Re-packaged zipped file of ALOS2 main data. Similar to AUIG2 format, but zipped once.  | `ALOS2*-YYMMDD-*1.5*.zip` (for L1.5) <br> `ALOS2*-YYMMDD-*2.1*` (for L2.1) <br> `ALOS2*-YYMMDD-*1.1*.zip` (for L1.1) |
    | Browse images | Only available for L1.5 and L2.1, browse images of geotiffs. |  `IMG-HX-ALOS2*_disp.browse.png`  |
