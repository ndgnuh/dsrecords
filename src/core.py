import os
import struct
import warnings
from functools import cached_property
from io import SEEK_END
from typing import Iterable, List, Optional

# Reserve for whatever changes in the future
RESERVED_SPACE = 1024


class IndexFile:
    def __init__(self, path: str):
        self.path = path

    def write(self, offsets: List[int]):
        with open(self.path, "wb") as io:
            n = len(offsets)
            io.write(struct.pack("<Q", n))
            for offset in offsets:
                io.write(struct.pack("<Q", offset))

    def __len__(self):
        if not os.path.isfile(self.path):
            return 0

        with open(self.path, "rb") as io:
            io.seek(0)
            (n,) = struct.unpack("<Q", io.read(8))
        return n

    def __getitem__(self, idx):
        assert idx < len(self)
        with open(self.path, "rb") as io:
            io.seek((idx + 1) * 8)
            (offset,) = struct.unpack("<Q", io.read(8))
        return offset

    def append(self, idx):
        n = len(self)
        mode = "wb" if n == 0 else "rb+"
        with open(self.path, mode) as io:
            # Increase length
            io.seek(0)
            io.write(struct.pack("<Q", n + 1))

            # Add index
            io.seek(0, SEEK_END)
            io.write(struct.pack("<Q", idx))


def make_dataset(
    record_iters: Iterable,
    output: str,
    serializers: List,
    index_path: Optional[str] = None,
):
    indices = []

    # Write record file
    with open(output, "wb") as io:
        io.seek(RESERVED_SPACE)

        for items in record_iters:
            # serialize
            items_bin = [serialize(items[i]) for i, serialize in enumerate(serializers)]
            headers = [len(b) for b in items_bin]
            headers_bin = [struct.pack("<Q", h) for h in headers]

            # Track global offset, local offset (size)
            indices.append(io.tell())

            # Write
            for h in headers_bin:
                io.write(h)
            for d in items_bin:
                io.write(d)

    # Write indice files
    if index_path is None:
        index_path = os.path.splitext(output)[0] + ".idx"
    IndexFile(index_path).write(indices)
    return output, index_path


class IndexedRecordDataset:
    def __init__(
        self,
        path: str,
        deserializers: Optional[List] = None,
        serializers: Optional[List] = None,
        index_path: Optional[str] = None,
    ):
        if index_path is None:
            index_path = os.path.splitext(path)[0] + ".idx"
        self.path = path
        self.deserializers = deserializers
        self.serializers = serializers
        self.index = IndexFile(index_path)

    @cached_property
    def num_items(self):
        return len(self.deserializers)

    def __iter__(self):
        return iter(self[i] for i in range(len(self)))

    def __len__(self):
        return len(self.index)

    def __getitem__(self, idx: int):
        msg = "You need de-serializers for reading the data"
        assert self.deserializers is not None, msg

        # Inputs
        offset = self.index[idx]
        fns = self.deserializers
        N = self.num_items

        # Deserialize
        with open(self.path, "rb") as io:
            io.seek(offset)
            lens = [struct.unpack("<Q", io.read(8))[0] for _ in range(N)]
            items = [fns[i](io.read(n)) for i, n in enumerate(lens)]

        return items

    def append(self, items):
        if not os.path.isfile(self.path) or len(self) == 0:
            with open(self.path, "wb") as io:
                fmt = "<" + "b" * RESERVED_SPACE
                io.write(struct.pack(fmt, *([0] * RESERVED_SPACE)))

        msg = "You need serializers for reading the data"
        assert self.serializers is not None, msg
        items_bin = [
            serialize(items[i]) for i, serialize in enumerate(self.serializers)
        ]
        headers = [len(b) for b in items_bin]
        headers_bin = [struct.pack("<Q", h) for h in headers]
        with open(self.path, "a+b") as io:
            io.seek(0, SEEK_END)
            idx = io.tell()
            self.index.append(idx)
            for b in headers_bin:
                io.write(b)
            for b in items_bin:
                io.write(b)


class EzRecordDataset(IndexedRecordDataset):
    def __post_init__(self):
        warnings.warning(
            "EzRecordDataset is deprecated due to name changes, use IndexedRecordDataset instead",
            DeprecationWarning,
        )
