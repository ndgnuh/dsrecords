import struct
from typing import Iterable, List


class IndexFile:
    def __init__(self, path: str):
        self.path = path

    def write(self, offsets: List[int]):
        with open(self.path, "w+b") as io:
            n = len(offsets)
            io.write(struct.pack("<Q", n))
            for offset in offsets:
                io.write(struct.pack("<Q", offset))

    def __len__(self):
        with open(self.path, "r+b") as io:
            (n,) = struct.unpack("<Q", io.read(8))
        return n

    def __getitem__(self, idx):
        assert idx < len(self)
        with open(self.path, "r+b") as io:
            io.seek((idx + 1) * 8)
            (offset,) = struct.unpack("<Q", io.read(8))
        return offset


def make_dataset(
    record_iters: Iterable,
    output: str,
    serialize_fns: List,
    index_path: Optional[str] = None,
):
    indices = []

    # Write record file
    with open(output, "wb") as io:
        for items in record_iters:
            # serialize
            items_bin = [
                serialize(items[i]) for i, serialize in enumerate(serialize_fns)
            ]
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
        index_path = path.splitext(output)[0] + ".idx"
    IndexFile(index_path).write(indices)
    return output, index_path


class EzRecordDataset:
    def __init__(
        self,
        path: str,
        deserialize_fns: List,
        index_path: Optional[str] = None,
    ):
        if index_path is None:
            index_path = os.path.splitext(path)[0] + ".idx"
        self.path = path
        self.deserialize_fns = deserialize_fns
        self.index = IndexFile(index_path)
        self.length = len(self.index)
        self.num_items = len(deserialize_fns)

    def __len__(self):
        return len(self.index)

    def __getitem__(self, idx: int):
        # Inputs
        offset = self.index[idx]
        fns = self.deserialize_fns
        N = self.num_items

        # Deserialize
        with open(self.path, "rb") as io:
            io.seek(offset)
            lens = [struct.unpack("<Q", io.read(8))[0] for _ in range(N)]
            items = [fns[i](io.read(n)) for i, n in enumerate(lens)]

        return items
