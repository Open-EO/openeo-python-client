import logging
from pprint import pprint

import shapely.geometry

import openeo
from openeo.rest.conversions import timeseries_json_to_pandas


def main():
    url = "https://openeo.vito.be"

    conn = openeo.connect(url)

    polygon = shapely.geometry.Polygon([(3.71, 51.01), (3.72, 51.02), (3.73, 51.01)])
    bbox = polygon.bounds

    result = (
        conn
            .load_collection("TERRASCOPE_S2_TOC_V2",
                             temporal_extent = ["2020-01-01", "2020-03-10"],
                             spatial_extent=dict(zip(["west", "south", "east", "north"], bbox)),
                             bands=["TOC-B04_10M","TOC-B08_10M"])
            .ndvi()
            .polygonal_mean_timeseries(polygon)
            .execute()
    )

    pprint(timeseries_json_to_pandas(result))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
