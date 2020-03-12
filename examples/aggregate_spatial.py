import logging
from pprint import pprint

import shapely.geometry

import openeo


def main():
    url = "http://openeo.vgt.vito.be/openeo/0.4.0"

    conn = openeo.connect(url)

    polygon = shapely.geometry.Polygon([(3.71, 51.01), (3.72, 51.02), (3.73, 51.01)])
    bbox = polygon.bounds

    result = (
        conn
            .load_collection("CGS_SENTINEL2_RADIOMETRY_V102_001")
            .filter_temporal("2020-01-01", "2020-03-10")
            .filter_bbox(crs="EPSG:4326", **dict(zip(["west", "south", "east", "north"], bbox)))
            .ndvi()
            .polygonal_mean_timeseries(polygon)
            .execute()
    )

    pprint(result)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
