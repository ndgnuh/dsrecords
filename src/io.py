import struct
from functools import partial, wraps
from io import BytesIO

NO_INPUT = object()


def curry(f):
    @wraps(f)
    def wrapped(x=NO_INPUT, **kwargs):
        if x is NO_INPUT:
            return partial(f, **kwargs)
        else:
            return f(x, **kwargs)

    return wrapped


def file_serialize(file_path: str) -> bytes:
    with open(file_path, "rb") as io:
        bs = io.read()
    return bs


@curry
def pil_serialize(image, format="JPEG", quality=95) -> bytes:
    io = BytesIO()
    image.save(io, format, quality=quality)
    image_bin = io.getvalue()
    io.close()
    image.close()
    return image_bin


def pil_deserialize(image_bin: bytes):
    from PIL import Image

    with BytesIO(image_bin) as io:
        image = Image.open(io)
        image.load()
    return image


def int32_serialize(n):
    return struct.pack("<l", n)


def int32_deserialize(n):
    return struct.unpack("<l", n)[0]


def float32_serialize(n):
    return struct.pack("<f", n)


def float32_deserialize(n):
    return struct.unpack("<f", n)[0]


@curry
def str_encode(s, encoding="utf-8"):
    return s.encode(encoding)


@curry
def str_decode(bs, encoding="utf-8"):
    return bs.decode(encoding)


def identity(bs):
    # Returns whatever
    return bs
