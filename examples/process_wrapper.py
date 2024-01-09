#!/usr/bin/env python2
import ast
import inspect
import logging
import os
import subprocess
from subprocess import CalledProcessError

logger = logging.getLogger(__name__)


def execute(
    out_dir,
    user,
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
    Version -- 1-hbo-AJ
    Author -- Alexander Jacob, Eurac Research, openEO
    Mission -- hackathon

    Inputs:
    user -- User -- 45/User String -- guest
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
    python-2

    Software Dependencies:
    python-2
    python-3
    openeo-0.4

    Processing Resources:
    ram -- 1
    disk -- 10
    cpu -- 1
    """
    args = [(arg, locals()[arg]) for arg in inspect.getargspec(execute)[0]]
    args = [value for pair in args for value in pair]
    path = os.path.dirname(os.path.realpath(__file__))
    logger.info("Script real path: %s", path)
    cmd = ["python3", os.path.join(path, "py3_process_wrapper-wcps_eurac.py")]
    cmd.extend(args)
    logger.info("Running python3 using command: \n%s\n", " ".join(cmd))
    output, returncode, error = "", 0, None
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True)
    except CalledProcessError as e:
        error, output, returncode = e, e.output, e.returncode
    logger.info(
        "Logs from command: "
        "\n\n############# PYTHON3 LOGS ################\n\n%s"
        "\n\n############# END PYTHON3 LOGS ################\n",
        output,
    )
    if returncode > 0:
        raise error
    # We expect the py3 wrapper to output in the last line the dictionary of outputs
    last_line = output.strip().split("\n")[-1]
    logger.info("Parsing last line: '%s'", last_line)
    outputs = ast.literal_eval(last_line)
    if not isinstance(outputs, dict):
        raise ValueError(
            "Output of python3 wrapper is not a dictionary of outputs, " "received type %s." % outputs.__class__
        )
    else:
        logger.info("Parsed outputs: \n%s", outputs)
    return outputs


if __name__ == "__main__":
    formatter = logging.Formatter(fmt="%(levelname)s - %(message)s")
    handlers = [logging.FileHandler("/tmp/process-wrapper.log"), logging.StreamHandler()]
    for handler in handlers:
        handler.setFormatter(formatter)
    level = logging.getLevelName(logging.DEBUG)
    logger.setLevel(level)
    for handler in handlers:
        logger.addHandler(handler)
    result = execute(
        out_dir="/tmp",
        user="guest",
        provider="Eurac Research",
        driver_url="https://openeo.eurac.edu",
        image_collection="S2_L2A_T32TPS_20M",
        bands="B03 B11",
        band_format="gtiff",
        # Rumst bbox w,s,e,n
        bbox_string="10.446624755859375, 46.72574176193996, 10.629272460937498, 46.845164430292755",
        temporal_extent="2016-06-28/2016-06-28",
    )
    print("result: ", result)
