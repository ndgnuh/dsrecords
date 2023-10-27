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
    name = "quick-remove-test.rec"
    data_orig = [0, 1, 2, 3, 4, 5]
    data_new = [0, 1, 2, 5, 4]
    make_dataset([[i] for i in data_orig], name, [io.save_int])
    data = IndexedRecordDataset(name, deserializers=[io.load_int])
    data.quick_remove_at(3)
    data_new_2 = [i[0] for i in data]
    assert all((x == y) for (x, y) in zip(data_new, data_new_2))
