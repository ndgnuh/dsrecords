import random
import tempfile

import pytest
from dsrecords import IndexedRecordDataset, io, make_dataset

save_uint32 = io.save_int(bits=32, signed=False)
load_uint32 = io.load_int(bits=32, signed=False)


def test_append_vs_create():
    dataset = [[random.randint(0, 2 ^ 64)] for _ in range(1000)]
    serializers = [save_uint32]

    # first write
    file_a = tempfile.NamedTemporaryFile("r+b")
    _, index_a = make_dataset(dataset, file_a.name, serializers)

    # second write
    file_b = tempfile.NamedTemporaryFile("r+b")
    data = IndexedRecordDataset(file_b.name, serializers=serializers)
    index_b = data.index.path
    for sample in dataset:
        data.append(sample)

    # Check
    with open(file_a.name, "rb") as f_a:
        with open(file_b.name, "rb") as f_b:
            assert f_a.read() == f_b.read()
    with open(index_a, "rb") as f_a:
        with open(index_b, "rb") as f_b:
            assert f_a.read() == f_b.read()
