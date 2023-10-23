# DSRecords

Dead simple RecordIO data format, inspired by Apache MXRecord and Protobuf.

## Installation

Install with `pip`:
```shell
pip install git+https://gitlab.com/ndgnuh/dsrecords.git
```


## Overview

- [Create a record dataset](#create-a-record-dataset)
- [Load a dataset](#load-a-dataset)
- [Supported data formats reference](formats.md)
- [Dataset format reference](records-format.md)

## Create a record dataset

There are two ways to create a dataset.
The first is to use the `make_dataset` function:

```python
import random
import math
from dsrecords import io, make_dataset

# Specify dataset
output_path = "my_data.rec"
n = 1000
xs = [random.uniform(-math.pi, math.pi) for _ in range(n)]
ys = [math.sin(x) for x in xs]
zs = [int(x) for x in xs]

# Setup the schema
serializers = [io.save_float, io.save_float, io.save_int]

# Generate the record dataset files
output_path, index_path = make_dataset(
    zip(xs, ys, zs),
    output_path,
    serializers
)
```

The second way is to use `IndexedRecordDataset` and add the samples incrementally:
```python
import random
import math
from dsrecords import io, IndexedRecordDataset

# Specify dataset
record_path = "my_data.rec"
n = 1000
xs = [random.uniform(-math.pi, math.pi) for _ in range(n)]
ys = [math.sin(x) for x in xs]
zs = [int(x) for x in xs]

# Setup the schema
serializers = [io.save_float, io.save_float, io.save_int]

# Generate the record dataset files
data = IndexedRecordDataset(record_path, serializers=serializers)
for x, y, z in zip(xs, ys, zs):
    data.append([x, y, z])
```

Use which-ever suitable for your usecase.
Either way, you need to provide a schema (a way to serialize your data).

After creation, there will be two files:
- `my_data.rec`: The data file, contains the data inself.
- `my_data.idx`: The index file, which tells the position of sample chunks inside the data file.


## Load a dataset

To load the created dataset, you will also need to provide a schema (in this case, the way to load your data).

```python
from dsrecords import io, IndexedRecordDataset

# Dataset file path
record_path = "my_data.rec"

# Specify schema
deserializers = [io.load_float, io.load_float, io.load_int]

# Load the data
data = IndexedRecordDataset(record_path, deserializers=deserializers)
print(data[0])
```

We separate serialization and deserialization because some formats can have multiple ways of serialization and vice-versa.

For example, you can read a JPEG file's content directly from disk (which is more efficient than loading the image and save it again, by the way) and store the binary blob as a record, but when deserialize, you read it with `Pillow` or `opencv` using Python's `BytesIO`.
