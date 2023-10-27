import os
import struct
import warnings
from copy import deepcopy
from functools import cached_property
from io import SEEK_END
from typing import Iterable, List, Optional

# Reserve for whatever changes in the future
RESERVED_SPACE = 1024
RESERVED_BYTES = struct.pack("<" + "x" * RESERVED_SPACE)


class IndexFile:
    """File object to interact with index file.

    Index file is the file that store the offsets
    to each of the data points in the respective data file.

    Args:
        path (str):
            Path to the index file. Does not need to exist.

    Attributes:
        path (str): Path to the physical index file.
    """

    def __init__(self, path: str):
        self.path = path

    def write(self, offsets: List[int]):
        """Write a list of offsets to the index file.

        !!! warning "This operation will create a new physical index file."
            *All the old offsets will be lost.* For adding new offsets without deleting the old ones, use `append` instead.

        Args:
            offsets (List[int]): List of offsets.
        """
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

    def __repr__(self):
        n = len(self)
        return f"Index file with {n} items"

    def append(self, offset: int):
        """Append offset to the index file.

        If the file does not exists, the file will be created.

        Args:
            offset (int): the offset to be added.
        """
        n = len(self)
        mode = "wb" if n == 0 else "rb+"
        with open(self.path, mode) as io:
            # Increase length
            io.seek(0)
            io.write(struct.pack("<Q", n + 1))

            # Add index
            io.seek(0, SEEK_END)
            io.write(struct.pack("<Q", offset))

    def quick_remove_at(self, idx: int):
        """Quickly remove an index by writing the index at the end to that index position.

        Conceptually, this operation do:
        ```python
        offsets[idx] = offsets.pop(-1)
        ```

        !!! warning "This operation does not preserve the position of the index"
            For example, the offset values `[0, 8, 16, 24, 32]` will be come `[0, 8, 32, 24]` if we remove the index `2`.

        To actually preserve the order, we would have to offset the
        content of the whole index file to the left, which is not
        cheap at all.

        Args:
            idx (int): The index to be removed
        """
        n = len(self)
        with open(self.path, "rb+") as f:
            # Take the offset at the end of the file
            f.seek(-8, SEEK_END)
            buffer = f.read(8)
            f.seek(-8, SEEK_END)
            f.truncate()

            # Overwrite current offset
            # If i is not the last one
            # no need for swapping
            if i < n - 1:
                f.seek(8 * (i + 1))
                f.write(buffer)

            # Reduce length
            f.seek(0)
            f.write(struct.pack("<Q", n - 1))

    def __setitem__(self, i, v):
        with open(self.path, "rb+") as f:
            # Overwrite current offset
            f.seek(8 * (i + 1))
            f.write(struct.pack("<Q", v))

    def __iter__(self):
        return (self[i] for i in range(len(self)))


def make_dataset(
    record_iters: Iterable,
    output: str,
    serializers: List,
    index_path: Optional[str] = None,
):
    indices = []

    # Write record file
    with open(output, "wb") as io:
        io.write(RESERVED_BYTES)

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
    """Wrapper object to work with record and index files.

    Attributes:
        path (str):
            Path to the dataset file.
        deserializers (Optional[List[Callable]]):
            A list of functions that take a `bytes` and return something.
            This is required for accessing data.
            Default: `None`.
        serializers (Optional[List[Callable]]):
            A list of functions that take something and return a `bytes`.
            This is required for appending new samples.
            Default: `None`.
        index_path (str):
            Path to the index file, will be guessed from `path`.
            For convenience, dataset file and index file normally
            have the same basename, only their extension are different.
            Default: `None`.
    """

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
        """Number of items in each data sample."""
        return len(self.deserializers)

    def quick_remove_at(self, idx):
        """Just a wrapper for `IndexFile.quick_remove_at`."""
        self.index.quick_remove_at(idx)

    def defrag(self, output_file: str):
        """Defragment the dataset.

        When you perform a lot of deletions, the dataset file becomes sparse.
        This operation create a new dataset file will no "holes" inside.
        The content will be preserved.

        !!! info "Defrag does not sort the index file"
            The `defrag` operation use the order inside the index file, so the index will not be sorted.
        """
        ref_data = deepcopy(self)
        ref_data.deserializers = [lambda x: x for _ in self.deserializers]
        serializers = [lambda x: x for _ in self.deserializers]

        def data_iter():
            for item in ref_data:
                yield item

        return make_dataset(data_iter(), output_file, serializers=serializers)

    def __iter__(self):
        """Iterate through this dataset"""
        return iter(self[i] for i in range(len(self)))

    def __len__(self):
        return len(self.index)

    def __getitem__(self, idx: int):
        msg = "You need de-serializers for reading the data"
        deserializers = self.deserializers
        assert deserializers is not None, msg

        # Inputs
        offset = self.index[idx]
        N = self.num_items

        # Deserialize
        with open(self.path, "rb") as io:
            io.seek(offset)
            lens = [struct.unpack("<Q", io.read(8))[0] for _ in range(N)]
            items = [deserializers[i](io.read(n)) for i, n in enumerate(lens)]

        return items

    def append(self, items: Tuple):
        """Append new items to the dataset.

        Serializers are required for appending new items.

        Args:
            items (Tuple): A single data sample.
        """
        if not os.path.isfile(self.path) or len(self) == 0:
            with open(self.path, "wb") as io:
                io.write(RESERVED_BYTES)

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
    """Deprecated, use IndexedRecordDataset instead"""

    def __post_init__(self):
        warnings.warning(
            "EzRecordDataset is deprecated due to name changes, use IndexedRecordDataset instead",
            DeprecationWarning,
        )
