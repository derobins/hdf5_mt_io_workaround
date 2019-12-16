/* Reader program for HDF5 multithreaded dataset I/O hack example */

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <hdf5.h>

#include "thpool.h"

#include "mt_hack.h"


int fd_g = -1;

int
verify(int *buf, int count)
{
    assert(buf);

    for (int i = 0; i < count; i++)
        if (buf[i] != i) {
            printf("BAD VERIFICATION! %d should be %d at index %d\n", buf[i], i, i);
            return -1;
        }

    return 0;
} /* verify */

int
normal(hid_t did, hid_t tid, hid_t msid, hid_t fsid)
{
    hsize_t offset = 0;
    hsize_t count = 0;

    int *buf = NULL;

    printf("H5Dread I/O calls\n");

    if (NULL == (buf = malloc(CHUNK_SIZE * sizeof(int))))
        goto error;

    count = CHUNK_SIZE;

    for (hsize_t u = 0; u < DSET_SIZE; u += CHUNK_SIZE) {

        offset = u;

        memset(buf, 0, CHUNK_SIZE * sizeof(int));

        if (H5Sselect_hyperslab(fsid, H5S_SELECT_SET, &offset, NULL, &count, NULL) < 0)
            goto error;

        if (H5Dread(did, tid, msid, fsid, H5P_DEFAULT, buf) < 0)
            goto error;

        if (verify(buf, CHUNK_SIZE) < 0)
            goto error;
    }

    free(buf);

    return 0;

error:
    free(buf);

    return -1;
} /* normal */

int
direct_chunk(hid_t did)
{
    hsize_t offset = 0;
    uint32_t mask = 0;

    int *buf = NULL;

    printf("H5Dread_chunk I/O calls\n");

    if (NULL == (buf = malloc(CHUNK_SIZE * sizeof(int))))
        goto error;

    for (hsize_t u = 0; u < DSET_SIZE; u += CHUNK_SIZE) {

        offset = u;

        memset(buf, 0, CHUNK_SIZE * sizeof(int));

        if (H5Dread_chunk(did, H5P_DEFAULT, &offset, &mask, buf) < 0)
            goto error;

        if (verify(buf, CHUNK_SIZE) < 0)
            goto error;
    }

    free(buf);

    return 0;

error:
    free(buf);

    return -1;
} /* direct chunk */

int
posix_io(hid_t did, hid_t fsid)
{
    hsize_t offset = 0;
    hsize_t nchunks = 0;
    uint32_t mask = 0;
    haddr_t addr = HADDR_UNDEF;
    hsize_t size = 0;

    int fd = -1;

    int *buf = NULL;

    printf("Single-threaded POSIX I/O calls\n");

    /* Open the HDF5 file for POSIX I/O */
    if ((fd = open(FILENAME, O_RDONLY)) < 0)
        goto error;

    if (NULL == (buf = malloc(CHUNK_SIZE * sizeof(int))))
        goto error;

    /* Get the number of chunks */
    if (H5Dget_num_chunks(did, fsid, &nchunks) < 0)
        goto error;

    /* Loop over all chunks */
    for (hsize_t u = 0; u < nchunks; u++) {

        offset = u;

        memset(buf, 0, CHUNK_SIZE * sizeof(int));

        /* Get the chunk size */
        addr = HADDR_UNDEF;
        size = 0;
        if (H5Dget_chunk_info_by_coord(did, &offset, &mask, &addr, &size) < 0)
            goto error;

        /* Read the data */
        if (pread(fd, buf, size, (off_t)addr) < 0)
            goto error;

        if (verify(buf, CHUNK_SIZE) < 0)
            goto error;
    }

    free(buf);

    if (close(fd) < 0)
        goto error;

    return 0;

error:
    free(buf);

    if (fd > -1)
        close(fd);

    return -1;
} /* posix_io */


typedef struct work_params_t {
    haddr_t addr;
    hsize_t size;
} work_params_t;

void
read_and_verify(void *arg)
{
    work_params_t *params = (work_params_t *)arg;

    int *buf = NULL;

    if (NULL == (buf = malloc(CHUNK_SIZE * sizeof(int))))
        goto error;

    /* Read the data */
    if (pread(fd_g, buf, params->size, (off_t)(params->addr)) < 0)
        goto error;

    if (verify(buf, CHUNK_SIZE) < 0)
        goto error;

    free(buf);

    return;

error:
    printf("BADNESS in callback! addr: %lu size: %llu\n", params->addr, params->size);
    free(buf);
    return;
}


int
multithreaded(hid_t did, hid_t fsid)
{
    hsize_t offset = 0;
    hsize_t nchunks = 0;
    uint32_t mask = 0;

    work_params_t *params = NULL;

    threadpool pool = NULL;

    printf("Multithreaded POSIX I/O calls\n");

    /* Create the thread pool */
    if (NULL == (pool = thpool_init(4)))
        goto error;

    /* Open the HDF5 file for POSIX I/O */
    if ((fd_g = open(FILENAME, O_RDONLY)) < 0)
        goto error;

    /* Get the number of chunks */
    if (H5Dget_num_chunks(did, fsid, &nchunks) < 0)
        goto error;

    /* Allocate a giant array to hold callback parameters */
    if (NULL == (params = calloc(nchunks, sizeof(work_params_t))))
        goto error;

    /* Loop over all chunks */
    for (hsize_t u = 0; u < nchunks; u++) {

        offset = u * CHUNK_SIZE;

        /* Get the chunk size */
        if (H5Dget_chunk_info_by_coord(did, &offset, &mask, &(params[u].addr), &(params[u].size)) < 0)
            goto error;

        /* Add a unit of work to the thread pool */
        if (thpool_add_work(pool, read_and_verify, (void *)&params[u]) < 0)
            goto error;
    }

    thpool_wait(pool);

    if (close(fd_g) < 0)
        goto error;

    thpool_destroy(pool); 

    free(params);

    return 0;

error:

    if (fd_g > -1)
        close(fd_g);

    if (pool)
        thpool_destroy(pool); 

    free(params);

    return -1;
} /* multithreaded */



int
main(int argc, char *argv[])
{
    hid_t fid = H5I_INVALID_HID;
    hid_t tid = H5I_INVALID_HID;
    hid_t did = H5I_INVALID_HID;
    hid_t msid = H5I_INVALID_HID;
    hid_t fsid = H5I_INVALID_HID;

    hsize_t dims = DSET_SIZE;
    hsize_t chunk_dims = CHUNK_SIZE;

    printf("HDF5 multithreaded I/O hack - reader\n");

    /***************************/
    /* Create/open HDF5 things */
    /***************************/

    if (H5I_INVALID_HID == (fid = H5Fopen(FILENAME, H5F_ACC_RDONLY, H5P_DEFAULT)))
        goto error;

    if (H5I_INVALID_HID == (tid = H5Tcopy(H5T_NATIVE_INT)))
        goto error;

    if (H5I_INVALID_HID == (fsid = H5Screate_simple(1, &dims, &dims)))
        goto error;

    if (H5I_INVALID_HID == (msid = H5Screate_simple(1, &chunk_dims, &chunk_dims)))
        goto error;

    if (H5I_INVALID_HID == (did = H5Dopen2(fid, DATASET_NAME, H5P_DEFAULT)))
        goto error;

    /************************/
    /* Read and verify data */
    /************************/

    /* H5Dread */
//    if (normal(did, tid, msid, fsid) < 0)
//        goto error;

    /* H5Ddirect_chunk */
//    if (direct_chunk(did) < 0)
//        goto error;

    /* Single-threaded version of multithreading hack */
//    if (posix_io(did, fsid) < 0)
//        goto error;

    /* Multithreading hack */
    if (multithreaded(did, fsid) < 0)
        goto error;

    /*********/
    /* Close */
    /*********/

    if (H5Tclose(tid) < 0)
        goto error;
    if (H5Sclose(msid) < 0)
        goto error;
    if (H5Sclose(fsid) < 0)
        goto error;
    if (H5Dclose(did) < 0)
        goto error;
    if (H5Fclose(fid) < 0)
        goto error;

    printf("DONE!\n");

    return EXIT_SUCCESS;

error:
    H5E_BEGIN_TRY {
        H5Tclose(tid);
        H5Sclose(msid);
        H5Sclose(fsid);
        H5Dclose(did);
        H5Fclose(fid);
    } H5E_END_TRY;

    printf("BADNESS!\n");

    return EXIT_FAILURE;
}
