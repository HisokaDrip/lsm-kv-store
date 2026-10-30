import os
import struct
import zlib

# record = crc32 | op | klen | vlen | key | value
# crc covers everything after itself, so a torn write at the tail just fails the check
_HDR = struct.Struct("<IBII")
PUT, DEL = 1, 2


class WAL:
    def __init__(self, path, sync=False):
        self.path = path
        self.sync = sync
        self._f = open(path, "ab")

    def append(self, op, key: bytes, value: bytes = b""):
        body = struct.pack("<BII", op, len(key), len(value)) + key + value
        self._f.write(struct.pack("<I", zlib.crc32(body)) + body)
        self._f.flush()
        if self.sync:
            os.fsync(self._f.fileno())

    def close(self):
        self._f.close()

    @staticmethod
    def replay(path):
        """Yield (op, key, value) until EOF or the first corrupt record."""
        if not os.path.exists(path):
            return
        with open(path, "rb") as f:
            data = f.read()
        pos = 0
        while pos + _HDR.size <= len(data):
            crc, op, klen, vlen = _HDR.unpack_from(data, pos)
            end = pos + _HDR.size + klen + vlen
            if end > len(data):
                break
            if zlib.crc32(data[pos + 4:end]) != crc:
                break
            kstart = pos + _HDR.size
            yield op, data[kstart:kstart + klen], data[kstart + klen:end]
            pos = end
