# hdf5_mt_io_workaround
An experimental program to perform multi-threaded I/O using the HDF5 library
and POSIX pread(2) calls to get around the global thread lock used to
protect the HDF5 library.

# Building

Use h5cc to build each test program. Note that the reader needs to link to
a third-party threadpool library I've included as a git submodule.

To build the submodule (in subdirectory):
```
gcc -c thpool.c -o thpool.c
ar rcs libthpool.a thpool.o
```

Then copy the header and static library to the main program directory.

To build the programs:
```
path/to/h5cc -o generator generator.c
path/to/h5cc -L. -lthpool -o reader reader.c
```

# Run

Run the generator first, then the reader. The generator creates the test file
and the reader opens it and reads it using three different forms of I/O:

* H5Dread calls
* H5Dread_chunk calls
* A single-threaded version of the multithreaded work-around
* The multithreaded work-around

The generator currently takes no command-line options. If you want to adjust
the size of the generated file or the dataset chunk size, you'll have to
modify the mt_work_around.h file.

The reader has options for the algorithm and number of threads (when using the
multithreaded work-around). Run reader -? to get an updated list of the options.

The multithreaded hack gets the address and size of the dataset's chunks from
the HDF5 library on the main thread and then fires off POSIX I/O read and
verify tasks for the thread pool to execute. This has the clever property of
allowing concurrent dataset I/O while only allowing one thread to be in the
at one time.
