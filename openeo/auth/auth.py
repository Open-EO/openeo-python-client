from abc import ABC


class Auth(ABC):
    """
    The Auth class represents a specific type of authentication method.
    Concrete implementations:
    :class:`BearerAuth`

    """

    def __init__(self, username, password, endpoint):
        """
        Initializes authentication with username and password at
        the given endpoint. To actually log in the user the login method has
        to be called.

        :param username: String Username credential of the user
        :param username: String Password credential of the user
        """
        self.username = username
        self.password = password
        self.endpoint = endpoint

    def login(self) -> bool:
        """
        Authenticates a user to the backend.

        :return: status: True if the login was successful, False if not.
        """
        pass

    def get_header(self) -> dict:
        """
        Returns needed header for a request of this authentication

        :return: header: dict consists of all arguments needed in the header.
        """
        pass

    def get_auth(self) -> dict:
        """
        Returns needed auth for a request of this authentication

        :return: auth: Authentication type (HTTP).
        """
        pass
