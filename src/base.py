import struct
from abc import ABC, abstractmethod
from typing import Generic, Iterable, List, Sequence, TypeVar

T = TypeVar("T")


class RecordFormat(ABC, Generic[T]):
    @abstractmethod
    def serialize(self, record: T) -> Sequence[bytes]:
        ...

    @abstractmethod
    def deserialize(record_bins: Sequence[bytes]) -> T:
        ...


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
    record_file: str,
    index_file: str,
    record_format: RecordFormat,
):
    indices = []

    # Write record file
    with open(record_file, "wb") as io:
        for record in record_iters:
            # serialize
            record_bin = record_format.serialize(record)

            # Track global offset, local offset (size)
            record_offsets = [io.tell()]
            for b in record_bin:
                io.write(b)
                offset = io.tell() - sum(record_offsets)
                record_offsets.append(offset)
            indices.append(record_offsets)

    # Write indice files
    indice_strs = [",".join(map(str, idx)) for idx in indices]
    with open(index_file, "w", encoding="utf-8") as io:
        io.write("\n".join(indice_strs))
    return record_file, index_file


class EzRecordDataset:
    def __init__(
        self,
        record_file: str,
        index_file: str,
        record_format: RecordFormat,
    ):
        super().__init__()
        self.record_file = record_file
        self.index_file = index_file
        self.fmt = record_format

        # Read index file
        with open(index_file, encoding="utf-8") as io:
            lines = io.readlines()
            lines = [line.strip() for line in lines]
            lines = [line for line in lines if len(line) > 0]
            offsets = [tuple(int(i) for i in line.split(",")) for line in lines]
        self.offsets = offsets

    def __len__(self):
        return len(self.offsets)

    def __getitem__(self, idx):
        # Offset, Size1, Size2...
        offsets = iter(self.offsets[idx])
        with open(self.record_file, "rb") as io:
            io.seek(next(offsets))
            record_bin = [io.read(sz) for sz in offsets]
            record = self.fmt.deserialize(record_bin)
        return record
