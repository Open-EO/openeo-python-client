import hashlib
import json
import logging
import re
import shutil
from pathlib import Path
from typing import Union, Optional, Callable

from openeo.rest.connection import Connection

_log = logging.getLogger(__name__)

# Alias for things that pathlib.Path() accepts
# TODO: move definition up higher
# TODO: does this render nicely in sphinx autodocs?
Pathable = Union[str, Path]


class LocalCache:
    # TODO: flush cache
    # TODO: trim cache
    # TODO: max cache size
    # TODO: doing 2 level cache key (connection hash + graph hash) is overkill, just use one level?
    # TODO: report cache size (bytes, files) from __init__
    # TODO: only cache success or also failures?
    DEFAULT_ROOT = ".openeo-local-cache"

    def __init__(self, root: Optional[Pathable] = None):
        self.root = Path(root or self.DEFAULT_ROOT)

    @classmethod
    def for_connection(cls, connection: Connection, root: Optional[Pathable] = None) -> "LocalCache":
        """Factory to create a LocalCache for a certain connection"""
        url = connection.root_url
        safe_name = re.sub("[^a-zA-Z0-9]+", "", url)
        h = hashlib.md5(url.encode("utf8")).hexdigest()[:6]
        root = Path(root or cls.DEFAULT_ROOT) / f"{safe_name}-{h}"
        _log.debug(f"Setting up local cache for connection: {root}")
        return cls(root=root)

    def cached_download(
            self,
            download: Callable[[dict, Path], Union[Path, bytes]],
            graph: dict,
            path: Optional[Pathable],
            print=print
    ):
        graph_json = json.dumps(graph, indent=None, separators=(',', ':'))
        graph_hash = hashlib.md5(graph_json.encode("utf8")).hexdigest()
        base_path = self.root / graph_hash
        base_path.mkdir(parents=True, exist_ok=True)
        metadata_path = base_path / ("_meta.json")
        cache_path = base_path / graph_hash
        if path:
            cache_path = cache_path.with_suffix(Path(path).suffix)
        # TODO: add more debug/info logging
        if metadata_path.exists() and cache_path.exists():
            # TODO: also check actual graph in metadata, or trust graph_hash
            print(f"Download cache hit for graph {graph_hash} ({cache_path})")
        else:
            print(f"Download cache miss for graph {graph_hash}: doing real download")
            with metadata_path.open("w") as f:
                # TODO: Store more metadata: start/end time of download, client version, used connection, ...
                metadata = {"graph": graph}
                json.dump(metadata, fp=f)
            download(graph=graph, outputfile=cache_path)
        if path:
            shutil.copy(cache_path, path)
        else:
            return cache_path.read_bytes()


class WorkBench(Connection):
    # TODO: factory to create from existing Connection?
    # TODO: is it possible to do it with composition instead of inheritance? (first experiments indicate it's not)
    # TODO: better name? Session, ConnectionWithLocalCache
    # TODO: instead of subclassing, inject this functionality directly in Connection?

    def __init__(
            self,
            url: str,
            *,
            download_dir: Pathable = ".",
            local_cache: Union[bool, LocalCache] = False,
            print=print,
            **kwargs,
    ):
        super().__init__(url, **kwargs)
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        if isinstance(local_cache, LocalCache):
            self.local_cache = local_cache
        elif isinstance(local_cache, (str, Path)):
            self.local_cache = LocalCache.for_connection(connection=self, root=local_cache)
        elif local_cache is True:
            self.local_cache = LocalCache.for_connection(connection=self)
        else:
            self.local_cache = None
        self.print = print

    def _real_download(self, graph: dict, outputfile: Union[Path, str, None] = None, timeout: int = 30 * 60):
        return super().download(graph=graph, outputfile=outputfile, timeout=timeout)

    def download(self, graph: dict, outputfile: Union[Path, str, None] = None, timeout: int = 30 * 60):
        if outputfile:
            outputfile = self.download_dir / outputfile

        # TODO: closure `real_download` instead of public method?
        if self.local_cache:
            self.local_cache.cached_download(download=self._real_download, graph=graph, path=outputfile)
        else:
            return self._real_download(graph=graph, outputfile=outputfile, timeout=timeout)
