"""This is the module that handles file serialization/deserialization.

Attributes:
    NO_INPUT (object):
        Immortal one-time object that is used by the `kurry` function to indicate there is no input.
"""
import struct
import warnings
from functools import lru_cache, partial, wraps
from io import BytesIO
from typing import Callable, List, Optional, TypeVar

T = TypeVar("T")
NO_INPUT = object()


@lru_cache
def _get_int_fmt(bits: int, signed: bool) -> str:
    """Return the integer format for python `struct` modules.

    The returned format is little-endian (has `<` prefix).

    Args:
        bits (int): The number bitness of the type, should be one of `8, 16, 32, 64, 128`.
        signed (bool): Whether the integer type is signed.
    """
    fmts = {8: "b", 16: "h", 32: "i", 64: "l", 128: "q"}
    assert bits in fmts.keys()
    fmt = fmts[bits]
    if not signed:
        fmt = fmt.upper()
    return f"<{fmt}"


@lru_cache
def _get_float_fmt(bits: int) -> str:
    """Return the float format for python `struct` modules.

    The returned format is little-endian (has `<` prefix).

    Args:
        bits (int): The number bitness of the type, should be one of `16, 32, 64`.
    """
    fmts = {16: "e", 32: "f", 64: "d"}
    assert bits in fmts.keys()
    fmt = fmts[bits]
    return f"<{fmt}"


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
            return partial(fn, **kwargs)
        else:
            return fn(x, **kwargs)

    return wrapped


def dump_file(file_path: str) -> bytes:
    """Read raw file contents.

    Args:
        file_path (str | Path):
            The file path (duh).
    """
    with open(file_path, "rb") as io:
        bs = io.read()
    return bs


@kurry
def dump_pil(image, format="JPEG", quality=95, **kwargs) -> bytes:
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
def dump_int(n: int, bits: int = 32, signed: bool = True):
    """Serialize int data.

    Args:
        n (int): Any integer

    Keyword Args:
        bits (int):
            Bitness of type, valid values are `8, 16, 32, 64, 128`, default: `32`.
        signed (bool):
            Store in signed format, default: `True`.
    """

    fmt = _get_int_fmt(bits, signed)
    return struct.pack(fmt, n)


@kurry
def dump_float(x: float, bits: int = 32):
    """Serialize floating point number.

    Args:
        x (int): Any float

    Keyword Args:
        bits (int):
            Bitness of type, valid values are ` 16, 32, 64`, default: `32`.
    """
    fmt = _get_float_fmt(bits)
    return struct.pack(fmt, x)


@kurry
def dump_str(s: str, encoding: str = "utf-8"):
    """Serialize string data

    Args:
        s (bytes): String data to be serialized.

    Keyword Args:
        encoding (str): String encoding (default: `utf-8`).
    """
    return s.encode(encoding)


@kurry
def dump_bool(b: bool):
    """Serialize bool data"""
    return struct.pack("<?", b)


@kurry
def load_bool(b: bool):
    """Deserialize bool data"""
    return struct.unpack("<?", b)[0]


@kurry
def load_int(data: bytes, bits: int = 32, signed: bool = True):
    """Deserialize integers, see `io.dump_int` for options."""
    fmt = _get_int_fmt(bits, signed)
    return struct.unpack(fmt, data)[0]


@kurry
def load_float(data: bytes, bits: int = 32):
    """Deserialize floats, see `io.dump_float` for options."""
    fmt = _get_float_fmt(bits)
    return struct.unpack(fmt, data)[0]


@kurry
def load_str(data: bytes, encoding: str = "utf-8"):
    """Deserialize string data

    Args:
        data (bytes): String data.

    Keyword Args:
        encoding (str): String encoding (default: `utf-8`).
    """
    return data.decode(encoding)


def dump_np(x):
    """Serialize a numpy array.

    Args:
        x (np.ndarray): The numpy array to beserialized"""
    import numpy as np

    with BytesIO() as io:
        np.save(io, x)
        bytes = io.getvalue()
    return bytes


def load_np(bs):
    """Deserialize a numpy array."""
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


@kurry
def dump_list(lst: List[T], dumper: Callable[T, bytes] = None, save_fn=None) -> bytes:
    """Serialize list of arbitrary items.

    Args:
        lst (List[T]): A list of items of type `T`.
        dumper (Callable[T, bytes]): The function to serialize data of type `T`.
    """
    if save_fn is not None:
        dumper = save_fn
        warnings.warn(
            "Please use `dumper` instead of `save_fn`, this option will be removed in the near future",
            DeprecationWarning,
        )
    # Because without length, the deserializer will have to read until read result is empty
    # a while-true loop in a repeatedly called function seems pretty cursed
    n = len(lst)
    with BytesIO() as f:
        length = struct.pack("<L", len(lst))
        f.write(length)
        for item in lst:
            data = dumper(item)
            header = struct.pack("<L", len(data))
            f.write(header)
            f.write(data)
        f.seek(0)
        bs = f.read()
    return bs


@kurry
def load_list(data: bytes, loader: Callable[bytes, T] = None, load_fn=None) -> List[T]:
    """Deserialize list of arbitrary items.

    Args:
        data (bytes): Raw data.
        loader (Callable[bytes, T]): The function to serialize data of type `T`.
    """

    if load_fn is not None:
        loader = load_fn
        warnings.warn(
            "Please use loader instead of load_fn, this option will be removed in the near future",
            DeprecationWarning,
        )

    def wrapped_deserializer(f):
        # Unsigned long is 4 bit
        header = f.read(4)
        (n,) = struct.unpack("<L", header)
        data = loader(f.read(n))
        return data

    with BytesIO(data) as f:
        (length,) = struct.unpack("<L", f.read(4))
        outputs = [wrapped_deserializer(f) for _ in range(length)]
    return outputs


@kurry
def dump_cv2(image, ext: str, flags: Optional[List] = None) -> bytes:
    """Use `cv2.imencode` to encode image to bytes.

    Args:
        image (np.ndarray): An image in np.ndarray format.
        ext (str): Image extension, there is no default.
        flags (Optional[List]): List of flags for `cv2.imencode`, default is `[]`.

    Example:
        ```python
        serialize = cv2_dump(ext='.jpeg')
        serialize(np.random.rand(300, 300, 3).astype('float32'))
        ```
    """
    import cv2

    flags = [] if flags is None else flags
    _, buf = cv2.imencode(ext, image)
    return buf.tobytes()


@kurry
def load_cv2(image_bin: bytes, flags: Optional[List] = None):
    """Use `cv2.imdecode` to decode image from bytes.

    Args:
        image_bin (bytes): Image data as bytes.
        flags: flags for `cv2.imread`, default to `cv2.IMREAD_UNCHANGED`.
    """
    import cv2
    import numpy as np

    if flags is None:
        flags = cv2.IMREAD_UNCHANGED

    image_bin = np.frombuffer(image_bin, np.uint8)
    image = cv2.imdecode(image_bin, flags)
    return image


# Deprecated serializers


for k, v in dict(locals()).items():
    if k.startswith("dump_"):
        _deprecated(k.replace("dump_", "save_"), v)
_deprecated("save_raw_file", dump_file)
