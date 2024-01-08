#!/usr/bin/env python3
import json
import logging
import sys
from datetime import datetime

import openeo

logger = logging.getLogger(__name__)


def execute(
    out_dir,
    user,
    password,
    provider,
    driver_url,
    image_collection,
    all_bands,
    bands,
    band_format,
    bbox_string,
    temporal_extent,
):
    """
    Identification:
    Name -- OpenEO PoC
    Description -- Retrieve Sentinel 2 bands in GeoTIFF
    Version -- 1-hbo
    Author -- Alexander Jacob, Eurac Research, openEO
    Mission -- hackathon

    Inputs:
    user -- User -- 45/User String -- guest
    password -- Password -- 45/User String -- guest_123
    provider -- Provider -- 45/User String -- Eurac Research
    driver_url -- Driver URL -- 45/User String -- https://openeo.eurac.edu
    image_collection -- Image Collection -- 45/User String -- S2_L2A_T32TPS_20M
    all_bands -- Bands -- 45/User String -- AOT B02 B03 B04 B05 B06 B07 B8A B11 B12 SCL VIS WVP CLD SNW
    bands -- Bands -- 45/User String -- B03 B11
    band_format -- Band Format -- 45/User String -- gtiff
    bbox_string -- BBox -- 45/User String -- 10.446624755859375, 46.72574176193996, 10.629272460937498, 46.845164430292755
    temporal_extent -- Temporal Extent -- 44/DateRange -- 2016-06-28T00:00:00.000Z/2016-06-28T00:00:00.000Z

    Outputs:
    band_dir -- Band Directory -- 45/User String
    band_files -- Band Files -- 45/User String

    Main Dependency:
    python-3

    Software Dependencies:
    openeo-0.4

    Processing Resources:
    ram -- 1
    disk -- 10
    cpu -- 1
    """
    all_bands = all_bands.split()
    bands = bands.split()
    temporal_extent = temporal_extent.split("/")
    west, south, east, north = list(map(lambda value: value.strip(), bbox_string.split(",")))
    bbox = {"west": west, "east": east, "south": south, "north": north}
    logger.info("Demo user: %s", user)
    logger.info("Provider: %s with driver URL %s", provider, driver_url)
    logger.info("Image Collection: %s", image_collection)
    logger.info("Bands: %s", bands)
    logger.info("Using BBox in str format - raw input (west, south, east, north): %s", bbox_string)
    logger.info("BBox: %s", bbox)
    logger.info("Temporal extent in string format start/end: %s", temporal_extent)
    connection = openeo.connect(driver_url)
    connection.authenticate_basic(user, password)

    logger.info(
        "describe_collection('%s'):\n %s\n",
        image_collection,
        json.dumps(connection.describe_collection(image_collection), indent=2),
    )
    cube = connection.load_collection(collection_id=image_collection, bands=all_bands)
    cube = cube.filter_bbox(**bbox)
    cube = cube.filter_temporal(extent=temporal_extent)

    logger.info("cube.to_json: \n%s\n", cube.to_json())

    logger.info("File Format: %s", band_format)
    logger.info(
        "File Name Format: {provider}.{image_collection}.{west}.{south}.{east}.{north}."
        "{temporal_extent[0]}.{temporal_extent[1]}.B{band.zfill(2)}.{band_format}"
    )
    band_dir = out_dir
    logger.info("Downloading bands in %s", out_dir)
    band_files = []
    for band in bands:
        cube_band = cube.band(band)
        band_file = (
            f"{provider}.{image_collection}.{west}.{south}.{east}.{north}."
            f"{temporal_extent[0]}.{temporal_extent[1]}.B{band.zfill(2)}.{band_format.lower()}"
        )
        logger.info("Downloading band %s: %s", band, band_file)
        band_path = f"{out_dir}/{band_file}"
        band_files.append(band_file)
        logger.info("Starting download at %s", datetime.now())
        cube_band.download(band_path, format=band_format)
        logger.info("Download finished for band %s at %s", band, datetime.now())

    logger.info("Downloads finished at %s", datetime.now())

    return {"band_dir": band_dir, "band_files": band_files}


def example():
    execute(
        out_dir="/tmp",
        user=None,
        password=None,
        provider="Eurac Research",
        driver_url="https://openeo.eurac.edu",
        image_collection="S2_L2A_T32TPS_20M",
        bands="B03 B11",
        band_format="gtiff",
        # Rumst bbox w,s,e,n
        bbox_string="10.446624755859375, 46.72574176193996, 10.629272460937498, 46.845164430292755",
        temporal_extent="2016-06-28/2016-06-28",
    )


if __name__ == "__main__":
    # Configure logger to add output to stdout/stderr
    formatter = logging.Formatter(fmt="%(levelname)s - %(message)s")
    handlers = [logging.StreamHandler()]
    for handler in handlers:
        handler.setFormatter(formatter)
    level = logging.getLevelName(logging.DEBUG)
    logger.setLevel(level)
    for handler in handlers:
        logger.addHandler(handler)
    # called from process_wrapper.py
    # py3_process_wrapper.py out_dir /tmp user eopen ...
    kwargs = {var: value for var, value in list(zip(*[sys.argv[i + 1 :: 2] for i in (0, 1)]))}
    logger.debug("kwargs from sys.argv: \n %s", kwargs)
    print(execute(**kwargs))
