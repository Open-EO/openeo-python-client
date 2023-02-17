import typing
from typing import Any, Dict, List, Union
from pathlib import Path

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo.rest.connection import Connection


class UserFile:
    """Represents a file in the user-workspace of openeo."""

    def __init__(self, path: str, connection: 'Connection', metadata: Dict[str, Any] = {}):
        self.path = path
        self.metadata = metadata
        self.connection = connection

    def __repr__(self):
        return '<{c} file={i!r}>'.format(c=self.__class__.__name__, i=self.path)

    def _get_endpoint(self) -> str:
        return "/files/{}".format(self.path)

    def get_metadata(self, key) -> Any:
        """ Get metadata about the file, e.g. file size (key: 'size') or modification date (key: 'modified')."""
        if key in self.metadata:
            return self.metadata[key]
        else:
            return None

    def download_file(self, target: Union[Path, str]) -> Path:
        """ Downloads a user-uploaded file."""
        # GET /files/{path}
        response = self.connection.get(self._get_endpoint(), expected_status=200, stream=True)

        path = Path(target)
        with path.open(mode="wb") as f:
            for chunk in response.iter_content(chunk_size=None):
                f.write(chunk)

        return path


    def upload_file(self, source: Union[Path, str]):
        # PUT /files/{path}
        """ Uploaded (or replaces) a user-uploaded file."""
        path = Path(source)
        with path.open(mode="rb") as f:
            self.connection.put(self._get_endpoint(), expected_status=200, data=f)

    def delete_file(self):
        """ Delete a user-uploaded file."""
        # DELETE /files/{path}
        self.connection.delete(self._get_endpoint(), expected_status=204)

    def to_dict(self) -> Dict[str, Any]:
        """ Returns the provided metadata as dict."""
        return self.metadata if "path" in self.metadata else {"path": self.path}