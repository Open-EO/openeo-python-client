from pathlib import Path
from typing import Union, Dict, Tuple

from openeo.rest import OpenEoClientException
from openeo.util import ensure_dir
from requests import Response


class Result:
    def __init__(self, job):
        self.job = job

    def download_file(self, target: Union[str, Path] = None) -> Path:
        filename, metadata = self._get_asset()
        url = metadata["href"]
 
        target = Path(target or Path.cwd())
        if target.is_dir():
            target = target / filename
 
        self._download_url(url, target)
        return target

    def download_files(self, target: Union[str, Path] = None) -> Dict[Path, dict]:
        target = Path(target or Path.cwd())
        if target.exists() and not target.is_dir():
            raise OpenEoClientException("The target argument must be a folder. Got {t!r}".format(t=str(target)))

        assets = {target / f: m for (f, m) in self._get_assets().items()}
        if len(assets) == 0:
            raise OpenEoClientException("Expected at least one result file to download, but got 0.")

        for path, metadata in assets.items():
            self._download_url(metadata["href"], path)

        return assets

    def _download_url(self, url: str, path: Path):
        ensure_dir(path.parent)
        with path.open('wb') as handle:
            # TODO: finetune download parameters/chunking?
            response = self.job.connection.get(url, stream=True)
            for block in response.iter_content(1024):
                if not block:
                    break
                handle.write(block)
        return path

    def load_json(self) -> dict:
        return self._get_response_object().json()

    def load_bytes(self) -> bytes:
        return self._get_response_object().content

    def _get_response_object(self) -> Response:
        filename, metadata = self._get_asset()
        url = metadata["href"]

        return self.job.connection.get(url, stream=True)

    def _get_asset(self) -> Tuple[str, dict]:
        assets = self._get_assets()
        if len(assets) != 1:
            raise OpenEoClientException(
                "Expected one result file to download, but got {c}: {u!r}".format(c=len(assets), u=assets))
        return assets.popitem()

    def _get_assets(self) -> Dict[str, dict]:
        results_url = "/jobs/{}/results".format(self.job.job_id)
        response = self.job.connection.get(results_url, expected_status=200).json()
        if "assets" in response:
            # API 1.0 style: dictionary mapping filenames to metadata dict (with at least a "href" field)
            return response["assets"]
        else:
            # Best effort translation of on old style to "assets" style (#134)
            return {a["href"].split("/")[-1]: a for a in response["links"]}
