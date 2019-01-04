from abc import ABC


class File(ABC):
    """Represents a file of openeo."""

    def __init__(self, connection, path):
        self.connection = connection
        self.path = path
        pass

    def download_file(self, target):
        """ Download a user file."""
        # GET /files/{user_id}/{path}
        pass

    def upload_file(self, source):
        """ Upload a user file."""
        # PUT /files/{user_id}/{path}
        pass

    def delete_file(self):
        """ Delete a user file."""
        # DELETE /files/{user_id}/{path}
        pass
