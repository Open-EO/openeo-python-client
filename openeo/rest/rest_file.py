
import os
import shutil

from openeo.file import File


class RESTFile(File):
    """Represents a file of openeo."""

    def download_file(self, target):
        """
        Downloads a user file to the back end.
        :param target: local path, where the file should be saved.
        :return: status: Response status code
        """
        # GET /files/{user_id}/{path}

        path = "/files/{}/{}".format(self.connection.userid, self.path)

        resp = self.connection.get(path, stream=True)

        if resp.status_code == 200:
            with open(target, 'wb') as f:
                shutil.copyfileobj(resp.raw, f)
            return resp.status_code
        else:
            return resp.status_code

    def upload_file(self, source):
        """
        Uploads a user file to the back end.
        :param source: Local path to the file that should be uploaded.
        :return: status: True if it was successful, False otherwise
        """
        if not os.path.isfile(source):
            return False

        if not self.path:
            self.path = os.path.basename(source)

        with open(source, 'rb') as f:
            input_file = f.read()

        path = "/files/{}/{}".format(self.connection.userid, self.path)

        content_type = {'Content-Type': 'application/octet-stream'}

        resp = self.connection.put(path=path, headers=content_type, data=input_file)

        return resp.status_code

    def delete_file(self):
        """
        Deletes a user file in the back end.
        :return: status: True if it was successful, False otherwise
        """
        path = "/users/{}/{}".format(self.connection.userid, self.path)

        resp = self.connection.delete(path)

        return resp.status_code
