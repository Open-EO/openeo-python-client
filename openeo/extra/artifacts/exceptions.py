class ArtifactsException(Exception):
    """
    Family of exceptions related to artifacts
    """


class NoArtifactsCapability(ArtifactsException):
    """
    There is no artifacts capability exposed by the backend
    """


class NoAdvertisedProviders(ArtifactsException):
    """
    The OpenEO backend does not advertise providers for artifacts storage
    """


class EmptyAdvertisedProviders(ArtifactsException):
    """
    The providers list is empty
    """

    def __str__(self):
        return """
The OpenEO backend used does not advertise providers for managing artifacts.
""".lstrip()


class UnsupportedArtifactsType(ArtifactsException):
    """
    The artifacts type is not supported
    """

    def __init__(self, type_id: str):
        self.type_id = type_id

    def __str__(self):
        return f"The OpenEO backend does not support {self.type_id}"


class NoDefaultConfig(ArtifactsException):
    def __init__(self, key: str):
        self.key = key

    def __str__(self):
        return f"There was no default config provided by backend for {self.key}"


class InvalidProviderCfg(ArtifactsException):
    """The backend has an invalid provider config. This must be fixed by the provider of the backend."""
