# Supported data formats

!!! warning "This section is not finished"
    There might be mistakes.

## Built-in serializers

**All** serializers **must** returns `bytes`.

::: io.identity
::: io.save_bool
::: io.save_int
::: io.save_float
::: io.save_str
::: io.save_raw_file
::: io.save_pil
::: io.save_np
::: io.save_cv2

## Built-in deserializers

::: io.identity
::: io.load_bool
::: io.load_int
::: io.load_float
::: io.load_str
::: io.load_pil
::: io.load_np
::: io.load_cv2

## List (de)serializers

!!! info "Version related notes"
    This functionality is available from `0.4.10`.

List (de)serizliers are higher order functions that helps you (de)serialize list of arbitrary items.

- These are intended to be used when you have non-numeric, variable length data, such as strings.
- The deserializer function is `io.load_list` and the serializer function is `io.save_list`.
- The list (de)serializers receive `load_fn` and `save_fn` keyword arguments respectively.
- The function `save_fn` is the function to serialize each of the items in the list.
- The function `load_fn` is the function to deserialize one item from raw bytes.

#### Basic example usage

Use it just like the other (de)serializers, just provide the item (de)serializer.
```python
txt = "The quick brown fox jumps over the lazy dog".split()
data_bin = io.save_list(txt, save_fn=io.save_str)
txt_ = io.load_list(data_bin, load_fn=io.load_str)
assert " ".join(txt) == " ".join(txt_)
```

#### Use with the dataset API

The list (de)serializers can also be partially applied:

```python
txt = "The quick brown fox jumps over the lazy dog".split()
save_fn = io.save_list(save_fn=io.save_str)
load_fn = io.load_list(load_fn=io.load_str)
txt_ = load_fn(save_fn(txt))
assert " ".join(txt) == " ".join(txt_)
```

#### Nested list

It is possible to create (de)serializers for nested list if you ever need one.
```python
# Data
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
```

#### List vs numpy

A quick "when to use which" list:

- *If you are lazy like me or efficiency does not matter that much*, use Numpy.
- *If you would like to tinker,* try both of these two on a small dataset and decide which one to use.
- Use Numpy if you have numerical, fixed length data, Numpy is more efficient in this case.
- Use list (de)serializers if you have non-numerical, variable length data, such as strings.
- In numerical the cases, Numpy array is faster and better.
- In complex objects cases, such as strings, list is more efficient, but might be a bit slower than Numpy.
- This was originally intended to be used with large image-and-text-mixed datasets.


::: io.save_list
::: io.load_list

## Write your own

To support your own format, you need to write a *serialize* and a deserializer.
A serializer takes what ever data you have and turns it to bytes.
The deserializer do the opposites.

In a nut shell, you need to write these functions:

```python
serialize_data(data: T) -> bytes
deserialize_data(data_bin: bytes) -> T
```

For example, this is how `numpy` (de)serializers are defined (`numpy` is lazy loaded in the actual source).
```python
import numpy as np


def save_np(x: np.ndarray):
    with BytesIO() as io:
        np.save(io, x)
        bytes = io.getvalue()
    return bytes


def load_np(bs):
    with BytesIO(bs) as io:
        x = np.load(io)
    return x
```

If you need extra options in your serializer, use the `io.kurry` function.
It makes a function curry if only keyword arguments is passed when calling.
For example, this is how a `PIL.Image` serializer can by defined:

```python
from io import BytesIO
from dsrecords import io


@io.kurry
def save_pil_jpeg(image, quality=95) -> bytes:
    io = BytesIO()
    image.save(io, "JPEG", quality=quality)
    image_bin = io.getvalue()
    io.close()
    image.close()
    return image_bin
```
