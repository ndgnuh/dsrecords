import random
import string
import tempfile
from os import path, remove

import pytest
from dsrecords import IndexedRecordDataset, io, make_dataset

tempdir = tempfile.TemporaryDirectory()


def tmpfile(suffix=".rec") -> str:
    rand_string = random.choices(string.ascii_uppercase + string.digits, k=10)
    rand_string = "".join(rand_string)
    rand_string = f"{rand_string}{suffix}"
    return path.join(tempdir.name, rand_string)


save_uint32 = io.save_int(bits=32, signed=False)
load_uint32 = io.load_int(bits=32, signed=False)


def test_append_vs_create():
    dataset = [[random.randint(0, 2 ^ 64)] for _ in range(1000)]
    serializers = [save_uint32]

    # first write
    file_a = tmpfile()
    _, index_a = make_dataset(dataset, file_a, serializers)

    # second write
    file_b = tmpfile()
    data = IndexedRecordDataset(file_b, serializers=serializers, create=True)
    index_b = data.index.path
    for sample in dataset:
        data.append(sample)

    # Check
    with open(file_a, "rb") as f_a:
        with open(file_b, "rb") as f_b:
            assert f_a.read() == f_b.read()
    with open(index_a, "rb") as f_a:
        with open(index_b, "rb") as f_b:
            assert f_a.read() == f_b.read()


def test_example_list_serializers():
    # Basic
    txt = "The quick brown fox jumps over the lazy dog".split()
    data_bin = io.save_list(txt, save_fn=io.save_str)
    txt_ = io.load_list(data_bin, load_fn=io.load_str)
    assert " ".join(txt) == " ".join(txt_)

    # Curry
    txt = "The quick brown fox jumps over the lazy dog".split()
    save_fn = io.save_list(save_fn=io.save_str)
    load_fn = io.load_list(load_fn=io.load_str)
    txt_ = load_fn(save_fn(txt))
    assert " ".join(txt) == " ".join(txt_)

    # Nested
    txt_list = [
        "The quick brown fox jumps over the lazy dog",
        "Do bạch kim rất quý nên sẽ dùng để lắm vô xương",
    ]
    nested_txt_list_1 = [txt.split(" ") for txt in txt_list]

    # Schemas
    save_fn = io.save_list(save_fn=io.save_list(save_fn=io.save_str))
    load_fn = io.load_list(load_fn=io.load_list(load_fn=io.load_str))

    # Serialize and deserialize
    data_bin = save_fn(nested_txt_list_1)
    nested_txt_list_2 = load_fn(data_bin)

    # Check
    txt_1 = "\n".join([" ".join(txt) for txt in nested_txt_list_1])
    txt_2 = "\n".join([" ".join(txt) for txt in nested_txt_list_2])
    assert txt_1 == txt_2


def test_quick_removal():
    # Quick removal
    # Don't test with floating points because they are cursed
    random.seed(0)
    n = 10000
    name = tmpfile()

    data_orig = [random.randint(0, n) for _ in range(n)]
    idx = random.randint(0, n - 1)

    # For manual checking
    # data_orig = list(range(8))
    # idx = [6, 2, 4]

    # Create dataset
    make_dataset([[i] for i in data_orig], name, [io.save_int])
    data = IndexedRecordDataset(name, deserializers=[io.load_int])

    # Remove index
    for _ in range(n // 2):
        idx = random.randint(0, n - 1)
        data.quick_remove_at(idx)
        if idx == n - 1:
            data_orig.pop(-1)
        else:
            data_orig[idx] = data_orig.pop(-1)
        n = n - 1

    # Edge index cases
    data.quick_remove_at(0)
    data_orig[0] = data_orig.pop(-1)
    data.quick_remove_at(len(data) - 1)
    data_orig.pop(len(data_orig) - 1)

    assert all((x[0] == y) for (x, y) in zip(data, data_orig))
    assert len(data) == len(data_orig)


def test_defrag():
    n = 10000
    name_1 = tempfile.NamedTemporaryFile("r+b").name
    name_2 = tempfile.NamedTemporaryFile("r+b").name

    # Create dataset
    data_orig = [random.randint(0, n) for _ in range(n)]
    make_dataset([[i] for i in data_orig], name_1, [io.save_float])
    data = IndexedRecordDataset(name_1, deserializers=[io.load_float])

    # Remove
    for i in range(n // 2):
        random.randint(0, n - 1)
        data.quick_remove_at(i)

    # Truncate
    data.defrag(name_2)
    data_defrag = IndexedRecordDataset(name_2, data.deserializers)

    # Obviously we don't want defrag to be useless...
    assert path.getsize(name_1) > path.getsize(name_2)
    assert len(data_defrag) == len(data)
    assert all((x[0] == y[0]) for (x, y) in zip(data, data_defrag))
