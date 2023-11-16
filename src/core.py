import os
import struct
import warnings
from copy import deepcopy
from functools import cached_property
from io import SEEK_CUR, SEEK_END
from pathlib import Path
from shutil import move
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

# Reserve for whatever changes in the future
RESERVED_SPACE = 1024
RESERVED_BYTES = struct.pack("<" + "x" * RESERVED_SPACE)
INDEX_SIZE = 8
INDEX_FMT = "<Q"


def init_index_file(output_path: str):
    """Create an "empty" index file

    Args:
        output_path (Union[str, Path]):
            Path to output index file, must not exists.
    """
    msg = f"The file {output_path} already exists"
    assert not os.path.exists(output_path), msg
    with open(output_path, "wb") as f:
        f.write(pack_index(0))
    return output_path


def init_data_file(output_path: Union[str, Path]):
    """Create an "empty" data file

    Args:
        output_path (Union[str, Path]):
            Path to output dataset file, must not exists.
    """
    msg = f"The file {output_path} already exists"
    assert not os.path.exists(output_path), msg
    with open(output_path, "wb") as f:
        f.write(RESERVED_BYTES)
    return output_path


def init_dataset(dataset_path: str, index_path: Optional[str] = None):
    """Create an empty dataset, include one data file and one index file.

    Args:
        dataset_path (Union[str, Path]):
            Path to output dataset file, must not exists.
        index_path (Union[str, Path, NoneType]):
            Path to output dataset file, must not exists.
            If is set to `None`, it will be determined by replacing the extension of
            dataset path with `.idx`.
            Default: `None`.
    """
    # Default index file
    if index_path is None:
        index_path = f"{os.path.splitext(dataset_path)[0]}.idx"

    # Name clash
    msg = "Name clash, dataset path should end with '.rec' extension, not '.idx'"
    assert index_path != dataset_path, msg

    # Create empty files
    init_index_file(index_path)
    init_data_file(dataset_path)
    return dataset_path, index_path


def pack_index(idx: int) -> bytes:
    """Convert a UInt64 to bytes buffer"""
    return struct.pack(INDEX_FMT, idx)


def pack_index(idx: int) -> bytes:
    """Convert a UInt64 to bytes buffer"""
    return struct.pack(INDEX_FMT, idx)


def unpack_index(idx_bin: bytes) -> int:
    """Decode a bytes buffer as UInt64"""
    return struct.unpack(INDEX_FMT, idx_bin)[0]


def pack_data(items: Tuple, serializers: List) -> bytes:
    """Serialize data to bytes with header and such

    Args:
        items (Tuple): Tuple of items.
        serializers (List[Callable]): List of serialize function.
    """
    iter_ = enumerate(items)
    items_bin = [serializers[i](item) for i, item in iter_]
    headers = [pack_index(len(b)) for b in items_bin]
    outputs = b"".join(headers + items_bin)
    return outputs


def unpack_headers_(io, n: int) -> List[int]:
    """Unpack headers from data. This function change the file pointer.

    Args:
        io: The file object
        n (int): Number of header items
    """
    return [unpack_index(io.read(INDEX_SIZE)) for i in range(n)]


def unpack_data_(io, headers: List[int], deserializers: List):
    """Deserialize data from io. This function change the file pointer.

    Args:
        io (File): The file object.
        headers (List[int]): List of item size.
        deserializers (List[Callable]): List of deserialize functions.
    """
    items = [deserializers[i](io.read(h)) for i, h in enumerate(headers)]
    return items


class IndexFile:
    """File object to interact with index file.

    Index file is the file that store the offsets
    to each of the data points in the respective data file.

    Args:
        path (str):
            Path to the index file.
        create (bool):
            Whether to create the a new index file.
            If true, the path must not exists.
            If false, the path must exists.
            Default: false.

    Attributes:
        path (str): Path to the physical index file.
    """

    def __init__(self, path: str, create: bool = False):
        exists = os.path.exists(path)
        if create:
            init_index_file(path)
        else:
            msg = f"The file {path} does not exists, to create a new one, use `create = true`"
            assert exists, msg
        self.path = path

    def _get_index_offset(self, idx: int):
        """Get the offset of the data-offset in the index file

        In the current format, it's `(i + 1) * INDEX_SIZE`.

        Args:
            idx (int): the data index.
        """
        return (idx + 1) * INDEX_SIZE

    def write(self, offsets: List[int]):
        """Write a list of offsets to the index file.

        !!! warning "This operation will create a new physical index file."
            *All the old offsets will be lost.* For adding new offsets without deleting the old ones, use `append` instead.

        Args:
            offsets (List[int]): List of offsets.
        """
        with open(self.path, "wb") as io:
            n = len(offsets)
            io.write(pack_index(n))
            for offset in offsets:
                io.write(pack_index(offset))

    def __len__(self):
        with open(self.path, "rb") as io:
            io.seek(0)
            n = unpack_index(io.read(INDEX_SIZE))
        return n

    def _remove_last(self, idx):
        n = len(self)
        with open(self.path, "rb+") as f:
            # Do not truncate the file because there are backswapped stuff
            # Write zeros so that it is not included in the backswap
            f.seek(self._get_index_offset(n - 1))
            f.write(pack_index(0))

            # Just reduce length
            f.seek(0)
            f.write(pack_index(n - 1))

    def _remove_with_backswap(self, idx: int):
        n = len(self)
        with open(self.path, "rb+") as f:
            # | n | i_0 | i_1 | ... i_(n-2) >|< i_(n-1) |
            back_offset = self._get_index_offset(n - 1)
            f.seek(back_offset)
            back_bin = f.read(INDEX_SIZE)

            # Swap
            cur_offset = self._get_index_offset(idx)
            f.seek(cur_offset)
            f.write(back_bin)

            # Reduce length
            f.seek(0)
            f.write(pack_index(n - 1))

    def remove_at(self, idx: int):
        """Remove data offset at some index.

        !!! warning "This function change the order of the records"
            Since the removal use backswapping, the record at the back will be swapped to the deletion index.
            To restore the order, trim the index file using `trim`.

        Args:
            idx (int): the index to be deleted.
        """
        n = len(self)
        assert idx < n and idx >= 0
        # TODO: remove with truncation
        if idx == n - 1:
            offset_bin = self._remove_last(idx)
        else:
            offset_bin = self._remove_with_backswap(idx)

    def get_backswap_offsets(self) -> Dict[int, int]:
        """Return list of offsets that are backswapped during deletion

        Returns:
            backswap (Dict[int, int]):
                The dict with the keys are the backswapped offsets,
                and the values are the index of those offsets.
        """
        n = len(self)
        offsets = []
        with open(self.path, "rb") as f:
            # Retrieve information
            a = self._get_index_offset(n)
            b = f.seek(0, SEEK_END)
            num_removed = (b - a) // INDEX_SIZE
            f.seek(a)

            # Retrieve indices
            for i in range(num_removed):
                idx = n + i
                offset = unpack_index(f.read(INDEX_SIZE))
                if offset != 0:
                    offsets.append(offset)
        offsets = sorted(offsets)
        return offsets

    def trim(self, output_file: str, replace: bool = False):
        """Truncate the index file, remove backswap bytes and restore order to the index file.

        Args:
            output_file (Optional[str]):
                The output index file. Must not be an existing path.
            replace (bool):
                If replace is true, the output file will be moved to the current index file
                on the disk. Default: false.
        """
        n = len(self)
        bs_offsets = self.get_backswap_offsets()

        # Filter out deleted index
        bs_maps = {offset: i for i, offset in enumerate(bs_offsets)}
        bs_offsets = []

        # Select which file to copy to
        new_file = IndexFile(output_file, create=True)
        for i in range(n):
            offset = self[i]
            # Skip back swapped index
            if offset in bs_maps:
                bs_offsets.append(offset)
                continue

            # Add index
            new_file.append(offset)

        # Add backswapped offsets
        for offset in bs_offsets:
            new_file.append(offset)
        # Replace
        if replace:
            move(output_file, self.path)

    def quick_remove_at(self, idx: int):
        """Deprecated, use `remove_at`"""
        self.remove_at(idx)

    def __getitem__(self, idx):
        with open(self.path, "rb") as io:
            io.seek(self._get_index_offset(idx))
            offset = unpack_index(io.read(INDEX_SIZE))
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
            io.write(pack_index(n + 1))

            # Add index
            io.seek(0, SEEK_END)
            io.write(pack_index(offset))

    def __setitem__(self, i, v):
        with open(self.path, "rb+") as f:
            # Overwrite current offset
            f.seek(self._get_index_offset(i))
            f.write(pack_index(v))

    def __iter__(self):
        return (self[i] for i in range(len(self)))


def make_dataset(
    record_iters: Iterable,
    output: str,
    serializers: List,
    index_path: Optional[str] = None,
):
    indices = []
    data = IndexedRecordDataset(
        output,
        index_path,
        create=True,
        serializers=serializers,
    )
    # Write record file
    for items in record_iters:
        data.append(items)

    # Return the paths
    data_path = data.path
    index_path = data.index.path
    return data_path, index_path


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
        create (bool):
            If create is true, attempt to create the dataset file and the index file.
            If not, simply use the existing files.
            In create mode, dataset file and index file must not exist.
            In normal mode, dataset file and index file must exist.
            Defaut: false.
    """

    def __init__(
        self,
        path: str,
        deserializers: Optional[List] = None,
        serializers: Optional[List] = None,
        index_path: Optional[str] = None,
        create: bool = False,
        transform: Optional[Callable] = None,
    ):
        if index_path is None:
            index_path = os.path.splitext(path)[0] + ".idx"
        if create:
            init_dataset(path, index_path)
        else:
            msg = f"Data file {path} does not exist, use `create = True` to create one"
            assert os.path.exists(path), msg
        self.path = path
        self.deserializers = deserializers
        self.serializers = serializers
        self.index = IndexFile(index_path)
        self.transform = transform

    @cached_property
    def num_items(self):
        """Number of items in each data sample."""
        return len(self.deserializers)

    def quick_remove_at(self, idx):
        """Just a wrapper for `IndexFile.remove_at`."""
        self.index.remove_at(idx)

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
        # first_offset = self.index[0]
        # length = len(self)
        # deserializers = self.deserializers
        # N = self.num_items
        # with open(self.path, "rb") as io:
        #     io.seek(first_offset)
        #     for _ in range(length):
        #         lens = [unpack_index(io.read(INDEX_SIZE)) for _ in range(N)]
        #         items = [deserializers[i](io.read(n)) for i, n in enumerate(lens)]
        #         yield items
        # <- Not thread safe
        N = len(self)
        return (self[i] for i in range(N))

    def __len__(self):
        return len(self.index)

    def __getitem__(self, idx: int):
        """Get data item"""
        # +-------------+
        # | Preparation |
        # +-------------+
        deserializers = self.deserializers
        transform = self.transform
        N = self.num_items

        # +-------------+
        # | Deserialize |
        # +-------------+
        if isinstance(idx, int):
            # +---------------+
            # | Full row data |
            # +---------------+
            offset = self.index[idx]
            with open(self.path, "rb") as io:
                io.seek(offset)
                lens = unpack_headers_(io, N)
                items = unpack_data_(io, lens, self.deserializers)
            return items
        else:
            # +-----------------------------+
            # | Partial mode, single column |
            # +-----------------------------+
            row_idx, col_idx = idx
            offset = self.index[row_idx]
            with open(self.path, "rb") as io:
                io.seek(offset)
                lens = unpack_headers_(io, N)
                i = 0
                for i in range(col_idx):
                    io.read(lens[i])
                data_bin = io.read(lens[col_idx])
                data = deserializers[col_idx](data_bin)
            return data

    def __setitem__(self, k, v):
        """Update data sample at some index.

        For now, this function just append the item to the end of the dataset
        and set the index to that offset.
        In one special case when the data is located at the end, overwrite the data
        without appending.
        In another special case when the update is smaller than the data, overwrite
        the data inplace.
        """
        # +---------+
        # | Prepare |
        # +---------+
        last_bytes = os.path.getsize(self.path)
        offset = self.index[k]
        N = self.num_items
        update_bin = pack_data(v, self.serializers)
        update_size = len(update_bin)

        # +------------------------------------------------+
        # | Case1: The update is smaller than current data |
        # +------------------------------------------------+
        def case_inplace(io):
            # print("Case inplace")
            io.seek(offset)
            io.write(update_bin)

        # +-------------------------------+
        # | Case2: The item is at the end |
        # +-------------------------------+
        def case_truncate(io):
            # print("Case truncate")
            io.seek(offset)
            io.truncate()
            io.write(update_bin)

        # +--------------------------------+
        # | Case 3: Generic, append to end |
        # +--------------------------------+
        def case_fallback(io):
            # print("Case fallback")
            io.seek(0, SEEK_END)
            new_offset = io.tell()
            io.write(update_bin)
            self.index[k] = new_offset

        with open(self.path, "rb+") as io:
            # +--------------------------------------------+
            # | Calculate total size, including the header |
            # +--------------------------------------------+
            io.seek(offset)
            headers = unpack_headers_(io, N)
            data_size = sum(headers) + INDEX_SIZE * N

            # +-------------------------------------------------------+
            # | Handle cases, the case when the item is at the end    |
            # | shoud have higher priority since it does not fragment |
            # | the data                                              |
            # +-------------------------------------------------------+
            if offset + data_size >= last_bytes:
                case_truncate(io)
            elif update_size <= data_size:
                case_inplace(io)
            else:
                case_fallback(io)

    def append(self, items: Tuple):
        """Append new items to the dataset.

        Serializers are required for appending new items.

        Args:
            items (Tuple): A single data sample.
        """
        data_bin = pack_data(items, self.serializers)
        with open(self.path, "a+b") as io:
            io.seek(0, SEEK_END)
            idx = io.tell()
            self.index.append(idx)
            io.write(data_bin)


class EzRecordDataset(IndexedRecordDataset):
    """Deprecated, use IndexedRecordDataset instead"""

    def __post_init__(self):
        warnings.warning(
            "EzRecordDataset is deprecated due to name changes, use IndexedRecordDataset instead",
            DeprecationWarning,
        )
