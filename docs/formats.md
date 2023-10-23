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

## Built-in deserializers

::: io.identity
::: io.load_bool
::: io.load_int
::: io.load_float
::: io.load_str
::: io.load_pil
::: io.load_np

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
