from openeo.auth import Auth
import requests
from requests.auth import HTTPBasicAuth


class BearerAuth(Auth):
    """
    Supports authentication using a bearer token.

    """

    def __init__(self, username, password, endpoint):
        """
        Initializes authentication with username and password at
        the given endpoint. To actually log in the user the login method has
        to be called.

        :param username: String Username credential of the user
        :param password: String Password credential of the user
        """
        self.token = None
        super(BearerAuth, self).__init__(username, password, endpoint)

    def login(self) -> bool:
        """
        Authenticates a user to the backend using bearer token. The token is
        then saved in the BearerAuth class, so that the operations are
        automatically made if the bearer token.

        :return: status: True if the login was successful, False if not.
        """
        token = requests.get(self.endpoint+'/credentials/basic',
                             auth=HTTPBasicAuth(self.username, self.password))

        if token.status_code == 200:
            self.token = token.json()["access_token"]
            return True
        else:
            return False

    def get_header(self) -> dict:
        """
        Returns needed header for a request of this authentication

        :return: header: dict consists of all arguments needed in the header.
        """
        return {'Authorization': 'Bearer {}'.format(self.token)}

    def get_auth(self) -> dict:
        """
        Returns needed auth for a request of this authentication

        :return: auth: Authentication type (HTTP).
        """
        return None
