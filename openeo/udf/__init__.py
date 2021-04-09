from openeo import BaseOpenEoException


class OpenEoUdfException(BaseOpenEoException):
    pass


from openeo.udf.feature_collection import FeatureCollection
from openeo.udf.structured_data import StructuredData
from openeo.udf.udf_data import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube
