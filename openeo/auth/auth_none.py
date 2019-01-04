from openeo.auth.auth import Auth


class NoneAuth(Auth):

    def login(self) -> bool:
        """
        Authenticates a user to the backend using bearer token. The token is
        then saved in the RESTSession class, so that the operations are
        automatically made if the bearer token.
        :param username: String Username credential of the user
        :param username: String Password credential of the user
        :return: status: True if the login was successful, False if not.
        """
        return True

    def get_header(self) -> dict:
        """
        Returns needed header for a request of this authentication
        :return: header: dict consists of all arguments needed in the header.
        """
        return {}

    def get_auth(self) -> dict:
        """
        Returns needed auth for a request of this authentication

        :return: auth: Authentication type (HTTP).
        """
        return None
