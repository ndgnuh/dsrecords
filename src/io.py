import struct
import warnings
from functools import partial, wraps
from io import BytesIO

NO_INPUT = object()


def _deprecated(f_name, new_f):
    @wraps(new_f)
    def new_f_wrapped(*args, **kwargs):
        msg = f"{f_name} is deprecated, please use {new_f.__name__} instead"
        with warnings.catch_warnings():
            warnings.simplefilter("once", DeprecationWarning)
            warnings.warn(msg, DeprecationWarning)
        return new_f(*args, **kwargs)

    globals()[f_name] = new_f_wrapped
    return new_f_wrapped


def kurry(fn):
    """Wraps a function so that all the call without the first arguments is a partial application.
    Useful to make (de)serializers with optional keyword arguments.

    Args:
        fn (callable): The function to be wrapped.

    Returns:
        wrapped_fn (callable): The wrapped function.

    Example:
        ```python
        @kurry
        def f(x, y = 0):
            return x + y

        g = f(y = 1)
        print(f(1)) # 1
        print(g(1)) # 2
        ```
    """

    @wraps(fn)
    def wrapped(x=NO_INPUT, **kwargs):
        if x is NO_INPUT:
            return partial(f, **kwargs)
        else:
            return f(x, **kwargs)

    return wrapped


def save_raw_file(file_path: str) -> bytes:
    """Read raw file contents.

    Args:
        file_path (str | Path):
            The file path (duh).
    """
    with open(file_path, "rb") as io:
        bs = io.read()
    return bs


@kurry
def save_pil(image, format="JPEG", quality=95, **kwargs) -> bytes:
    """Serialize a `Pillow.Image` to bytes.

    Any `Image.save`'s keyword arguments can be used by this function.

    Args:
        image (PIL.Image): The image to be serialized.

    Keyword Args:
        format (str): Image format (default: JPEG)
        quality (int): The quality for lossy formats (default: 95)
    """
    io = BytesIO()
    image.save(io, format, quality=quality, **kwargs)
    image_bin = io.getvalue()
    io.close()
    image.close()
    return image_bin


def load_pil(image_bin: bytes):
    """Load a Pillow.Image from raw bytes."""
    from PIL import Image

    with BytesIO(image_bin) as io:
        image = Image.open(io)
        image.load()
    return image


@kurry
def save_int(n, bits=32, signed=True):
    return struct.pack("<l", n)


@kurry
def save_float(n, bits=32):
    return struct.pack("<f", n)


@kurry
def save_str(s, encoding="utf-8"):
    return s.encode(encoding)


@kurry
def load_int(data, bits=32):
    return struct.unpack("<l", data)[0]


@kurry
def load_float(n, bits=32):
    return struct.unpack("<f", n)[0]


@kurry
def load_str(str_bin, encoding="utf-8"):
    return str_bin.decode(encoding)


def save_np(x):
    import numpy as np

    with BytesIO() as io:
        np.save(io, x)
        bytes = io.getvalue()
    return bytes


def load_np(bs):
    import numpy as np

    with BytesIO(bs) as io:
        x = np.load(io)
    return x


def identity(bs: bytes) -> bytes:
    """Does not do anything, incase what you load or save is already in `bytes`.

    Args:
        bs (bytes): Raw bytes.
    """
    return bs


# Deprecated serializers


_deprecated("file_serialize", save_raw_file)
_deprecated("pil_serialize", save_pil)
_deprecated("np_save", save_np)


def save_int(n, bits=32, signed=True):
    return struct.pack("<l", n)
