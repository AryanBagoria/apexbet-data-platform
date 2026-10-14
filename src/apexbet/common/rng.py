"""Deterministic RNG factory so every generator run is reproducible."""

from __future__ import annotations

import random


def rng_for(seed: int, *namespace: object) -> random.Random:
    """Return a Random seeded by (seed, namespace...).

    Uses a stable hash (blake2b over the string form) so the same seed and
    namespace always produce the same stream regardless of Python's hash
    randomization.
    """
    import hashlib

    material = "|".join([str(seed), *(str(n) for n in namespace)]).encode("utf-8")
    digest = hashlib.blake2b(material, digest_size=8).digest()
    return random.Random(int.from_bytes(digest, "big"))
