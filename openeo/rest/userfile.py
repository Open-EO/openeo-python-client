import typing
from typing import Any, Dict, Union
import os
from pathlib import Path
from openeo.util import ensure_dir

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo.rest.connection import Connection


class UserFile:
    """Represents a file in the user-workspace of openeo."""

    def __init__(self, path: str, connection: 'Connection', metadata: Dict[str, Any] = None):
        self.path = path
        self.metadata = metadata or {"path": path}
        self.connection = connection

    def __repr__(self):
        return '<{c} file={i!r}>'.format(c=self.__class__.__name__, i=self.path)

    def _get_endpoint(self) -> str:
        return "/files/{}".format(self.path)

    def download(self, target: Union[Path, str] = None) -> Path:
        """
        Downloads a user-uploaded file to the given location.

         :param target: download target path. Can be an existing folder 
             (in which case the file name advertised by backend will be used) 
             or full file name. By default, the working directory will be used.
        """
        # GET /files/{path}
        response = self.connection.get(self._get_endpoint(), expected_status=200, stream=True)

        target = Path(target or Path.cwd()) 
        if target.is_dir():
            target = target / os.path.basename(self.path)
        ensure_dir(target.parent)
        
        with target.open(mode="wb") as f:
            for chunk in response.iter_content(chunk_size=None):
                f.write(chunk)

        return target


    def upload(self, source: Union[Path, str]):
        # PUT /files/{path}
        """ Uploaded (or replaces) a user-uploaded file."""
        path = Path(source)
        with path.open(mode="rb") as f:
            self.connection.put(self._get_endpoint(), expected_status=200, data=f)

    def delete(self):
        """ Delete a user-uploaded file."""
        # DELETE /files/{path}
        self.connection.delete(self._get_endpoint(), expected_status=204)

    def to_dict(self) -> Dict[str, Any]:
        """ Returns the provided metadata as dict."""
        return self.metadata