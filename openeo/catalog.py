from abc import ABC, abstractmethod
from typing import List


class EOProduct(ABC):
    """Class representing a single Earth Observation product. """

    @property
    @abstractmethod
    def collection_identifier(self) -> str:
        """ Returns the collection identifier """
        pass

    @property
    @abstractmethod
    def sensor_type(self) -> str:
        """ Sensor type property, can be:
        * OPTICAL
        * RADAR
        * ALTIMETRIC
        * ATMOSPHERIC
        * LIMB
        """
        pass

    @property
    @abstractmethod
    def footprint(self):
        """ The target location observed during the EarthObservation
        """
        pass

    @property
    @abstractmethod
    def uri(self) -> List[str]:
        """ The URI of the product, returned as a list as a single product may consiste of multiple files.
        TODO this location and the type of filesystem is system dependent

        """
        pass
