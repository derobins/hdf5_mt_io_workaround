/* Header for HDF5 multithreaded dataset I/O hack example */

#define FILENAME "data.h5"
#define DATASET_NAME "data"

/* 1D dataset: default 4G elements, 1K element chunks */
//#define DSET_SIZE        4294967296
#define DSET_SIZE   1073741824
#define CHUNK_SIZE  1048576
