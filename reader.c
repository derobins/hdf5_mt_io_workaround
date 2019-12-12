/* Reader program for HDF5 multithreaded dataset I/O hack example */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>

#include <hdf5.h>

#include "mt_hack.h"

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
direct_chunk(hid_t did, hid_t tid, hid_t msid, hid_t fsid)
{
    hsize_t offset = 0;
    uint32_t mask = 0;

    int *buf = NULL;

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
multithreaded(hid_t did, hid_t tid, hid_t msid, hid_t fsid)
{
    return 0;
} /* multithreaded */



int
main(int argc, char *argv[])
{
    hid_t fid = H5I_INVALID_HID;
    hid_t tid = H5I_INVALID_HID;
    hid_t did = H5I_INVALID_HID;
    hid_t msid = H5I_INVALID_HID;
    hid_t fsid = H5I_INVALID_HID;

    hsize_t dims[NDIMS] = {DSET_SIZE};
    hsize_t chunk_dims[NDIMS] = {CHUNK_SIZE};

    printf("HDF5 multithreaded I/O hack - reader\n");

    /***************************/
    /* Create/open HDF5 things */
    /***************************/

    if (H5I_INVALID_HID == (fid = H5Fopen(FILENAME, H5F_ACC_RDONLY, H5P_DEFAULT)))
        goto error;

    if (H5I_INVALID_HID == (tid = H5Tcopy(H5T_NATIVE_INT)))
        goto error;

    if (H5I_INVALID_HID == (fsid = H5Screate_simple(NDIMS, dims, dims)))
        goto error;

    if (H5I_INVALID_HID == (msid = H5Screate_simple(NDIMS, chunk_dims, chunk_dims)))
        goto error;

    if (H5I_INVALID_HID == (did = H5Dopen2(fid, DATASET_NAME, H5P_DEFAULT)))
        goto error;

    /************************/
    /* Read and verify data */
    /************************/

    /* H5Dread */
    if (normal(did, tid, msid, fsid) < 0)
        goto error;

    /* H5Ddirect_chunk */
    if (direct_chunk(did, tid, msid, fsid) < 0)
        goto error;

    /* Multithreading hack */
    if (multithreaded(did, tid, msid, fsid) < 0)
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
