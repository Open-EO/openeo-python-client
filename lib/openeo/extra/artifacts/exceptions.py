class ArtifactsException(Exception):
    """
    Family of exceptions related to artifacts
    """


class NoAdvertisedProviders(ArtifactsException):
    """
    The providers list is empty. This OpenEO backend does not seem to support artifacts or at least not advertise its
    capabilities.
    """

    def __init__(self):
        super().__init__("The OpenEO backend used does not advertise providers for managing artifacts.")


class UnsupportedArtifactsType(ArtifactsException):
    """
    The artifacts type is not supported. This should only occur if you specify config manually and the artifact type
    is not supported by the backend.

    :parameter type_id: The type_id that is not supported but which was attempted to be used
    """

    def __init__(self, type_id: str):
        super().__init__(f"The OpenEO backend does not support {type_id}")
        self.type_id = type_id


class NoDefaultConfig(ArtifactsException):
    """
    This OpenEO backend does not seem to advertise a default value for a config parameter. This is likely a bug in the
    capabilities advertised by the backend. If you are not specify config manually you should contact support of the
    backend provider.

    :parameter key: The key for which no config value was found
    """

    def __init__(self, key: str):
        super().__init__(f"There was no default config provided by backend for {key}")
        self.key = key


class InvalidProviderConfig(ArtifactsException):
    """
    The backend has an invalid provider config. This must be fixed by the provider of the backend.
    If you are not specify config manually you should contact support of the backend provider.
    """


class ProviderSpecificException(ArtifactsException):
    """
    This is an exception specific to the type of artifact that is used. The exception itself will contain additional
    information.
    """
