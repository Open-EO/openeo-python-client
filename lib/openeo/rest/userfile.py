from __future__ import annotations

import typing
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Optional, Union

from openeo.rest import DEFAULT_DOWNLOAD_CHUNK_SIZE
from openeo.util import ensure_dir

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo.rest.connection import Connection


class UserFile:
    """
    Handle to a (user-uploaded) file in the user workspace on a openEO back-end.
    """

    def __init__(
        self,
        path: Union[str, PurePosixPath, None],
        *,
        connection: Connection,
        metadata: Optional[dict] = None,
    ):
        if path:
            pass
        elif metadata and metadata.get("path"):
            path = metadata.get("path")
        else:
            raise ValueError(
                "File path should be specified through `path` or `metadata` argument."
            )

        self.path = PurePosixPath(path)
        self.metadata = metadata or {"path": path}
        self.connection = connection

    @classmethod
    def from_metadata(cls, metadata: dict, connection: Connection) -> UserFile:
        """Build :py:class:`UserFile` from a workspace file metadata dictionary."""
        return cls(path=None, connection=connection, metadata=metadata)

    def __repr__(self):
        return "<{c} file={i!r}>".format(c=self.__class__.__name__, i=self.path)

    def _get_endpoint(self) -> str:
        return f"/files/{self.path!s}"

    def download(self, target: Union[Path, str] = None) -> Path:
        """
        Downloads a user-uploaded file from the user workspace on the back-end
        locally to the given location.

        :param target: local download target path. Can be an existing folder
             (in which case the file name advertised by backend will be used)
             or full file name. By default, the working directory will be used.
        """
        response = self.connection.get(
            self._get_endpoint(), expected_status=200, stream=True
        )

        target = Path(target or Path.cwd())
        if target.is_dir():
            target = target / self.path.name
        ensure_dir(target.parent)

        with target.open(mode="wb") as f:
            for chunk in response.iter_content(chunk_size=DEFAULT_DOWNLOAD_CHUNK_SIZE):
                f.write(chunk)

        return target

    def upload(self, source: Union[Path, str]) -> UserFile:
        """
        Uploads a local file to the path corresponding to this :py:class:`UserFile` in the user workspace
        and returns new :py:class:`UserFile` of newly uploaded file.

            .. tip::
                Usually you'll just need
                :py:meth:`Connection.upload_file() <openeo.rest.connection.Connection.upload_file>`
                instead of this :py:class:`UserFile` method.

        If the file exists in the user workspace it will be replaced.

        :param source: A path to a file on the local file system to upload.
        :return: new :py:class:`UserFile` instance of the newly uploaded file
        """
        return self.connection.upload_file(source, target=self.path)

    def delete(self):
        """Delete the user-uploaded file from the user workspace on the back-end."""
        self.connection.delete(self._get_endpoint(), expected_status=204)

    def to_dict(self) -> Dict[str, Any]:
        """Returns the provided metadata as dict."""
        # This is used in internal/jupyter.py to detect and get the original metadata.
        # TODO: make this more explicit with an internal API?
        return self.metadata
