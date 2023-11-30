
# Dead Simple Records


Easy to hack and dead simple RecordIO-like data storing. Inspired by MXNet's RecordIO and Protobuf.

- [Documentations](https://ndgnuh.gitlab.io/dsrecords/)

## (planned) features

- [x] Binary-based data format with index file
- [x] Easy custom serialization schemes
- [x] Common serialization schemes (more TBA)
- [x] Documentation
- [x] Append, update, delete data sample
- [ ] Pack/unpack form
- [ ] Schema serialization

## Quick start
See the overview section in the [documentations](https://ndgnuh.gitlab.io/dsrecords/).

Also, check out [this notebook](https://github.com/ndgnuh/ezrecords/blob/master/Examples.ipynb) for a quick example, this notebook has not been updated in a while though.

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
