from abc import ABC, abstractmethod
from typing import List,Dict
import cloudpickle
from .sessions import Session
import base64


class ImageCollection(ABC):
    """Class representing an Image Collection. """

    session: Session
    graph: Dict

    bands: List
    dates: List

    def __init__(self, parentgraph:Dict,session:Session):
        self.graph = parentgraph
        self.session = session


    def combinebands(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        pickled_lambda = cloudpickle.dumps(bandfunction)
        graph = {
            'process_id': 'band_arithmetic',
            'args' : {
                'imagery':self.graph,
                'bands':bands,
                'function': str(base64.b64encode(pickled_lambda),"UTF-8")
            }
        }
        return ImageCollection(graph,session=self.session)

    ####VIEW methods #######
    def meanseries(self, x,y, srs="EPSG:4326") -> Dict:
        """
        Extract a time series for the given point location.
        :param x: The x coordinate of the point
        :param y: The y coordinate of the point
        :param srs: The spatial reference system of the coordinates, by default this is 'EPSG:4326', where x=longitude and y=latitude.
        :return: Dict: A timeseries
        """
        return self.session.point_timeseries(self.graph, x, y, srs)


    def geotiff(self, bbox="",time=""):
        """Extraxts a geotiff from this image collection."""
        pass
