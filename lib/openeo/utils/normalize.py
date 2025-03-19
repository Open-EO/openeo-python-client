from typing import Tuple, Union


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
