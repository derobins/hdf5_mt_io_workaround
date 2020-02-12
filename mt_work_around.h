/* Header for HDF5 multithreaded dataset I/O work-around example */

#ifndef _mt_work_around_H
#define _mt_work_around_H

#define DATASET_NAME "data"

/* 1 TiB = 1,099,511,627,776 bytes
 * @32 bits per element: 274,877,906,944 elements
 * @64 bits per element: 68,719,476,736 elements
 *
 * For smaller systems:
 * 4,294,967,296 elements @ 32 bits per element = 17,179,869,184 bytes (16 GiB)
 * 1,073,741,824 elements @ 32 bits per element = 4,294,967,296 bytes (4 GiB)
 */

/* 1D dataset, size in elements */
//#define DSET_SIZE        274877906944       /* 1 TiB for 32-bit datatypes */
//#define DSET_SIZE        68719476736        /* 1 TiB for 64-bit datatypes, 512 GiB for 32-bit */
//#define DSET_SIZE        4294967296         /* 16 GiB file for smaller systems */
#define DSET_SIZE        1073741824         /* 4 GiB file for testing */

/* Chunk size, in elements (set low to force a lot of thread activity) */
#define CHUNK_SIZE  1048576

#endif /* _mt_work_around_H */

