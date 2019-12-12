# hdf5_mt_io_hack
An experimental program to perform multi-threaded I/O using the HDF5 library

# Building

Use h5cc to build each test program. Note that the reader needs to link to
the pthread library.

```
path/to/h5cc -o generator generator.c
path/to/h5cc -lpthread -o reader reader.c
```

# Run

Run the generator first, then the reader. The generator creates the test file
and the reader opens it and reads it using three different forms of I/O:

* H5Dread calls
* H5Dread_chunk calls
* A multithreaded hack

```
./generator
./reader
```
