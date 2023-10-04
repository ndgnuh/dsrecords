# Dead Simple Records

Easy to hack and dead simple RecordIO-like data storing. Inspired by MXNet's RecordIO and Protobuf.

## (planned) features

- [x] Binary-based data format with index file
- [x] Easy custom serialization schemes
- [x] Common serialization schemes (more TBA)
- [ ] Documentation

## Quick start

See [this notebook](https://github.com/ndgnuh/ezrecords/blob/master/Examples.ipynb) for how to use the library.

## Motivation

TBA

## How does it work?

#### Saving

1. Open an empty file, this is the data file.
2. Take whatever data you got, serialize it in a way (for example, write to PNG buffer)
3. Write the serialized data to the said file buffer, store the offset and size of the data
4. Repeat until all data are written
5. Write all the offsets and sizes to another file (index file)

#### Loading

1. Load the data file and the index file.
2. When given an index, use the index file to get the offset and size of the data.
3. Seek the file pointer to the offset, read the exact size and deserialize the data.

## Prebuilt format


## Customized serialization format
