from typing import Callable, Iterable, Optional, Tuple, Union


def normalize_resample_resolution(
    resolution: Union[int, float, Tuple[float, float], Tuple[int, int]]
) -> Tuple[Union[int, float], Union[int, float]]:
    """Normalize a resolution value, as used in the `resample_spatial` process to a two-element tuple."""
    if isinstance(resolution, (int, float)):
        return (resolution, resolution)
    elif (
        isinstance(resolution, (list, tuple))
        and len(resolution) == 2
        and all(isinstance(r, (int, float)) for r in resolution)
    ):
        return tuple(resolution)
    raise ValueError(f"Invalid resolution {resolution!r}")


def unique(iterable, key: Optional[Callable] = None) -> Iterable:
    """Deduplicate an iterable based on a key function."""
    # TODO: also support non-hashable items?
    seen = set()
    key = key or (lambda x: x)
    for x in iterable:
        k = key(x)
        if k not in seen:
            seen.add(k)
            yield x
