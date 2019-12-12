/* Generator program for HDF5 multithreaded dataset I/O hack example */

#include <stdio.h>
#include <stdlib.h>

#include <hdf5.h>

#include "mt_hack.h"

int
main(int argc, char *argv[])
{
    hid_t fid = H5I_INVALID_HID;
    hid_t tid = H5I_INVALID_HID;
    hid_t dcpl_id = H5I_INVALID_HID;
    hid_t did = H5I_INVALID_HID;
    hid_t msid = H5I_INVALID_HID;
    hid_t fsid = H5I_INVALID_HID;

    hsize_t dims[NDIMS] = {DSET_SIZE};
    hsize_t chunk_dims[NDIMS] = {CHUNK_SIZE};

    hsize_t offset = 0;
    hsize_t count = 0;

    int *buf = NULL;

    printf("HDF5 multithreaded I/O hack - generator\n");

    /**********************/
    /* Create HDF5 things */
    /**********************/

    if (H5I_INVALID_HID == (fid = H5Fcreate(FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT)))
        goto error;

    if (H5I_INVALID_HID == (tid = H5Tcopy(H5T_NATIVE_INT)))
        goto error;

    if (H5I_INVALID_HID == (fsid = H5Screate_simple(NDIMS, dims, dims)))
        goto error;

    if (H5I_INVALID_HID == (msid = H5Screate_simple(NDIMS, chunk_dims, chunk_dims)))
        goto error;

    if (H5I_INVALID_HID == (did = H5Dcreate2(fid, DATASET_NAME, tid, fsid, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)))
        goto error;

    /**************/
    /* Write data */
    /**************/

    if (NULL == (buf = malloc(CHUNK_SIZE * sizeof(int))))
        goto error;
    for (int i = 0; i < CHUNK_SIZE; i++)
        buf[i] = i;

    count = CHUNK_SIZE;

    for (hsize_t u = 0; u < DSET_SIZE; u += CHUNK_SIZE) {

        offset = u;

        if (H5Sselect_hyperslab(fsid, H5S_SELECT_SET, &offset, NULL, &count, NULL) < 0)
            goto error;

        if (H5Dwrite(did, tid, msid, fsid, H5P_DEFAULT, buf) < 0)
            goto error;
    }

    /*********/
    /* Close */
    /*********/

    free(buf);

    if (H5Tclose(tid) < 0)
        goto error;
    if (H5Sclose(msid) < 0)
        goto error;
    if (H5Sclose(fsid) < 0)
        goto error;
    if (H5Pclose(dcpl_id) < 0)
        goto error;
    if (H5Dclose(did) < 0)
        goto error;
    if (H5Fclose(fid) < 0)
        goto error;

    printf("DONE!\n");

    return EXIT_SUCCESS;

error:
    H5E_BEGIN_TRY {

        free(buf);

        H5Tclose(tid);
        H5Sclose(msid);
        H5Sclose(fsid);
        H5Pclose(dcpl_id);
        H5Dclose(did);
        H5Fclose(fid);
    } H5E_END_TRY;

    printf("BADNESS!\n");

    return EXIT_FAILURE;
}
