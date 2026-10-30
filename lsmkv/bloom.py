import hashlib
import math


class BloomFilter:
    """Plain bit-array bloom filter using double hashing (Kirsch-Mitzenmacher)."""

    def __init__(self, n_items, fp_rate=0.01, *, nbits=None, k=None, bits=None):
        if nbits is None:
            n_items = max(1, n_items)
            nbits = int(-n_items * math.log(fp_rate) / (math.log(2) ** 2))
            nbits = max(8, nbits)
            k = max(1, round(nbits / n_items * math.log(2)))
        self.nbits = nbits
        self.k = k
        self.bits = bits if bits is not None else bytearray((nbits + 7) // 8)

    def _positions(self, key: bytes):
        d = hashlib.blake2b(key, digest_size=16).digest()
        h1 = int.from_bytes(d[:8], "little")
        h2 = int.from_bytes(d[8:], "little") | 1  # odd so it cycles the whole range
        for i in range(self.k):
            yield (h1 + i * h2) % self.nbits

    def add(self, key: bytes):
        for p in self._positions(key):
            self.bits[p >> 3] |= 1 << (p & 7)

    def __contains__(self, key: bytes):
        return all(self.bits[p >> 3] & (1 << (p & 7)) for p in self._positions(key))

    def to_bytes(self) -> bytes:
        return self.nbits.to_bytes(4, "little") + bytes([self.k]) + bytes(self.bits)

    @classmethod
    def from_bytes(cls, raw: bytes):
        nbits = int.from_bytes(raw[:4], "little")
        return cls(0, nbits=nbits, k=raw[4], bits=bytearray(raw[5:]))
