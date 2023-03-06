import typing
from typing import Any, Dict, Union, Optional
from pathlib import Path, PurePosixPath
from openeo.util import ensure_dir

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo.rest.connection import Connection


class UserFile:
    """Represents a file in the user-workspace of openeo."""

    def __init__(
        self,
        path: Union[str, PurePosixPath, None],
        *,
        connection: "Connection",
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
    def from_metadata(cls, metadata: dict, connection: "Connection") -> "UserFile":
        """Build :py:class:`UserFile` from workspace file metadata dictionary."""
        return cls(path=None, connection=connection, metadata=metadata)

    def __repr__(self):
        return '<{c} file={i!r}>'.format(c=self.__class__.__name__, i=self.path)

    def _get_endpoint(self) -> str:
        return f"/files/{self.path!s}"

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
            target = target / self.path.name
        ensure_dir(target.parent)

        with target.open(mode="wb") as f:
            for chunk in response.iter_content(chunk_size=None):
                f.write(chunk)

        return target

    def upload(self, source: Union[Path, str]) -> "UserFile":
        """
        Uploads a file to the given target location in the user workspace.

        If the file exists in the user workspace it will be replaced.

        :param source: A path to a file on the local file system to upload.
        """
        # PUT /files/{path}
        return self.connection.upload_file(source, target=self.path)

    def delete(self):
        """ Delete a user-uploaded file."""
        # DELETE /files/{path}
        self.connection.delete(self._get_endpoint(), expected_status=204)

    def to_dict(self) -> Dict[str, Any]:
        """
        Returns the provided metadata as dict.

        This is used in internal/jupyter.py to detect and get the original metadata.
        """
        return self.metadata
