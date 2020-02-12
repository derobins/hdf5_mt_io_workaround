/* Reader program for HDF5 multithreaded dataset I/O work-around example */

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <hdf5.h>

#include "thpool.h"

#include "mt_work_around.h"

typedef enum algorithm_e {
    HDF5_DEFAULT = 0,
    DIRECT_CHUNK,
    POSIX_ST,
    POSIX_MT
} algorithm_e;


/* Globals */

/* File descriptor for the POSIX access */
int fd_g = -1;

/* Whether or not to show thread execution times */
bool show_thread_times_g = false;

/* Whether or not to show thread bandwidths */
bool show_thread_bandwidths_g = false;

/* Function to convert timespec struct to nanoseconds */
uint64_t
ns_from_timespec(struct timespec ts)
{
    return (ts.tv_sec * 1000 * 1000 * 1000) + ts.tv_nsec;
}

void
print_elapsed_sec(struct timespec start_ts, struct timespec end_ts)
{
    uint64_t start_ns = ns_from_timespec(start_ts);
    uint64_t end_ns = ns_from_timespec(end_ts);

    double sec = (end_ns - start_ns) / 1E9;

    printf("%f s", sec);
}

void
print_elapsed_sec_thread(struct timespec start_ts, struct timespec end_ts)
{
    uint64_t start_ns = ns_from_timespec(start_ts);
    uint64_t end_ns = ns_from_timespec(end_ts);

    double sec = (end_ns - start_ns) / 1E9;

    /* In lieu of a real producer-consumer log, put all the printf I/O into 
     * a single statement to minimize interleave.
     */
    printf("%f s\tThread execution time (via CLOCK_THREAD_CPUTIME_ID)\n", sec);
}

/* KiB, MiB, GiB, TiB, PiB, EiB - Used in profiling and timing code */
#define H5_KB (1024.0)
#define H5_MB (1024.0 * 1024.0)
#define H5_GB (1024.0 * 1024.0 * 1024.0)
#define H5_TB (1024.0 * 1024.0 * 1024.0 * 1024.0)
#define H5_PB (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0)
#define H5_EB (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0)

void
print_bandwidth(uint64_t n_bytes, struct timespec start_ts, struct timespec end_ts)
{
    uint64_t start_ns = ns_from_timespec(start_ts);
    uint64_t end_ns = ns_from_timespec(end_ts);
    double n_seconds = (end_ns - start_ns) / (double)(1000 * 1000 * 1000);

    char    buf[32];
    double	bw;

    bw = n_bytes / n_seconds;

    if(0 == n_bytes)
        strcpy(buf, "0.000  B/s");

    else if(bw < 1.0)
        sprintf(buf, "%10.4e", bw);

    else if(bw < H5_KB) {
        sprintf(buf, "%05.4f", bw);
        strcpy(buf + 5, "  B/s");
    }

    else if(bw < H5_MB) {
        sprintf(buf, "%05.4f", bw / H5_KB);
        strcpy(buf + 5, " kB/s");
    }

    else if(bw < H5_GB) {
        sprintf(buf, "%05.4f", bw / H5_MB);
        strcpy(buf + 5, " MB/s");
    }

    else if(bw < H5_TB) {
        sprintf(buf, "%05.4f", bw / H5_GB);
        strcpy(buf + 5, " GB/s");
    }

    else if(bw < H5_PB) {
        sprintf(buf, "%05.4f", bw / H5_TB);
        strcpy(buf + 5, " TB/s");
    }

    else if(bw < H5_EB) {
        sprintf(buf, "%05.4f", bw / H5_PB);
        strcpy(buf + 5, " PB/s");
    }

    else {
        sprintf(buf, "%10.4e", bw);
        sprintf(buf, "%10.3e", bw);
    }

    printf("%s\n", buf);

} /* print_bandwidth() */

int
verify(uint32_t *buf, uint32_t val, int count)
{
    assert(buf);

    for (int i = 0; i < count; i++)
        if (buf[i] != val) {
            printf("BAD VERIFICATION! %ul should be %ul at index %d\n", buf[i], val, i);
            return -1;
        }

    return 0;
} /* verify */

int
hdf5_default(hid_t did, hid_t tid, hid_t msid, hid_t fsid)
{
    hsize_t offset = 0;
    hsize_t count = 0;
    uint32_t chunk_n = 0;
    uint32_t *buf = NULL;

    printf("H5Dread I/O calls\n");

    if (NULL == (buf = malloc(CHUNK_SIZE * sizeof(uint32_t))))
        goto error;

    count = CHUNK_SIZE;
    chunk_n = 0;

    for (hsize_t u = 0; u < DSET_SIZE; u += CHUNK_SIZE) {

        offset = u;

        memset(buf, 0, CHUNK_SIZE * sizeof(uint32_t));

        if (H5Sselect_hyperslab(fsid, H5S_SELECT_SET, &offset, NULL, &count, NULL) < 0)
            goto error;

        if (H5Dread(did, tid, msid, fsid, H5P_DEFAULT, buf) < 0)
            goto error;

        if (verify(buf, chunk_n, CHUNK_SIZE) < 0)
            goto error;

        chunk_n++;
    }

    free(buf);

    return 0;

error:
    free(buf);

    return -1;
} /* hdf5_default */

int
direct_chunk(hid_t did)
{
    hsize_t offset = 0;
    uint32_t mask = 0;
    uint32_t chunk_n = 0;
    uint32_t *buf = NULL;

    printf("H5Dread_chunk I/O calls\n");

    if (NULL == (buf = malloc(CHUNK_SIZE * sizeof(uint32_t))))
        goto error;

    chunk_n = 0;

    for (hsize_t u = 0; u < DSET_SIZE; u += CHUNK_SIZE) {

        offset = u;

        memset(buf, 0, CHUNK_SIZE * sizeof(uint32_t));

        if (H5Dread_chunk(did, H5P_DEFAULT, &offset, &mask, buf) < 0)
            goto error;

        if (verify(buf, chunk_n, CHUNK_SIZE) < 0)
            goto error;

        chunk_n++;
    }

    free(buf);

    return 0;

error:
    free(buf);

    return -1;
} /* direct chunk */

int
posix_single_thread(hid_t did, hid_t fsid, const char *filename)
{
    hsize_t offset = 0;
    hsize_t nchunks = 0;
    uint32_t mask = 0;
    haddr_t addr = HADDR_UNDEF;
    hsize_t size = 0;

    int fd = -1;

    uint32_t chunk_n = 0;
    uint32_t *buf = NULL;

    printf("Single-threaded POSIX I/O calls\n");

    /* Open the HDF5 file for POSIX I/O */
    if ((fd = open(filename, O_RDONLY)) < 0)
        goto error;

    if (NULL == (buf = malloc(CHUNK_SIZE * sizeof(int))))
        goto error;

    /* Get the number of chunks */
    if (H5Dget_num_chunks(did, fsid, &nchunks) < 0)
        goto error;

    /* Loop over all chunks */

    chunk_n = 0;

    for (hsize_t u = 0; u < nchunks; u++) {

        offset = u * CHUNK_SIZE;

        memset(buf, 0, CHUNK_SIZE * sizeof(uint32_t));

        /* Get the chunk size */
        addr = HADDR_UNDEF;
        size = 0;
        if (H5Dget_chunk_info_by_coord(did, &offset, &mask, &addr, &size) < 0)
            goto error;

        /* Read the data */
        if (pread(fd, buf, size, (off_t)addr) < 0)
            goto error;

        if (verify(buf, chunk_n, CHUNK_SIZE) < 0)
            goto error;

        chunk_n++;
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
} /* posix_single_thread */


typedef struct work_params_t {
    uint32_t chunk_n;
    haddr_t addr;
    hsize_t size;
} work_params_t;

void
read_and_verify(void *arg)
{
    work_params_t *params = (work_params_t *)arg;

    struct timespec thread_start_ts;
    struct timespec thread_end_ts;

    uint32_t *buf = NULL;

    /* START THREAD TIMER */
    if (show_thread_times_g || show_thread_bandwidths_g)
        if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &thread_start_ts) < 0)
            goto error;

    if (NULL == (buf = malloc(CHUNK_SIZE * sizeof(uint32_t))))
        goto error;

    /* Read the data */
    if (pread(fd_g, buf, params->size, (off_t)(params->addr)) < 0)
        goto error;

    if (verify(buf, params->chunk_n, CHUNK_SIZE) < 0)
        goto error;

    free(buf);

    /* STOP THREAD TIMER */
    if (show_thread_times_g || show_thread_bandwidths_g)
        if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &thread_end_ts) < 0)
            goto error;

    /* Print timing and/or bandwidth */
    if (show_thread_times_g)
        print_elapsed_sec_thread(thread_start_ts, thread_end_ts);
    if (show_thread_bandwidths_g)
        print_bandwidth(params->size, thread_start_ts, thread_end_ts);

    return;

error:
    printf("BADNESS in callback! addr: %lu size: %llu\n", params->addr, params->size);
    free(buf);
    return;
}


int
posix_multithreaded(hid_t did, hid_t fsid, const char *filename, int n_threads)
{
    hsize_t offset = 0;
    hsize_t nchunks = 0;
    uint32_t mask = 0;
    uint32_t chunk_n = 0;

    struct timespec start_ts;
    struct timespec end_ts;

    work_params_t *params = NULL;

    threadpool pool = NULL;

    printf("Multithreaded POSIX I/O calls\n");

    /* Create the thread pool */
    if (clock_gettime(CLOCK_MONOTONIC, &start_ts) < 0)
        goto error;
    if (NULL == (pool = thpool_init(n_threads)))
        goto error;
    printf("Number of threads: %d\n", n_threads);
    if (clock_gettime(CLOCK_MONOTONIC, &end_ts) < 0)
        goto error;
    print_elapsed_sec(start_ts, end_ts);
    printf("\tTime to start thread pool (via CLOCK_MONOTONIC)\n");

    /* Open the HDF5 file for POSIX I/O */
    if ((fd_g = open(filename, O_RDONLY)) < 0)
        goto error;

    /* Get the number of chunks */
    if (H5Dget_num_chunks(did, fsid, &nchunks) < 0)
        goto error;

    /* Allocate a giant array to hold callback parameters */
    if (NULL == (params = calloc(nchunks, sizeof(work_params_t))))
        goto error;

    /* Loop over all chunks */

    chunk_n = 0;

    if (clock_gettime(CLOCK_MONOTONIC, &start_ts) < 0)
        goto error;
    for (hsize_t u = 0; u < nchunks; u++) {

        offset = u * CHUNK_SIZE;

        /* Get the chunk size */
        if (H5Dget_chunk_info_by_coord(did, &offset, &mask, &(params[u].addr), &(params[u].size)) < 0)
            goto error;

        params[u].chunk_n = chunk_n;

        /* Add a unit of work to the thread pool */
        if (thpool_add_work(pool, read_and_verify, (void *)&params[u]) < 0)
            goto error;

        chunk_n++;
    }
    if (clock_gettime(CLOCK_MONOTONIC, &end_ts) < 0)
        goto error;
    print_elapsed_sec(start_ts, end_ts);
    printf("\tTime spent launching threads (via CLOCK_MONOTONIC)\n");

    if (clock_gettime(CLOCK_MONOTONIC, &start_ts) < 0)
        goto error;
    thpool_wait(pool);
    if (clock_gettime(CLOCK_MONOTONIC, &end_ts) < 0)
        goto error;
    print_elapsed_sec(start_ts, end_ts);
    printf("\tTime spent waiting for all threads to finish (via CLOCK_MONOTONIC)\n");

    if (close(fd_g) < 0)
        goto error;

    if (clock_gettime(CLOCK_MONOTONIC, &start_ts) < 0)
        goto error;
    thpool_destroy(pool); 
    if (clock_gettime(CLOCK_MONOTONIC, &end_ts) < 0)
        goto error;
    print_elapsed_sec(start_ts, end_ts);
    printf("\tTime to destroy thread pool (via CLOCK_MONOTONIC)\n");

    free(params);

    return 0;

error:

    if (fd_g > -1)
        close(fd_g);

    if (pool)
        thpool_destroy(pool); 

    free(params);

    return -1;
} /* posix_multithreaded */


void
usage(void)
{
    printf("\n");
    printf("HDF5 multi-threaded I/O work-around - reader\n");
    printf("Reads and verifies the data in the generated file.\n");
    printf("(Run after running the generator program)\n");
    printf("\n");
    printf("The four algorithms are:\n");
    printf("\n");
    printf("default - Uses H5Dread to read the data.\n");
    printf("          This is the default so you don't need to specify this explicitly.\n");
    printf("\n");
    printf("directchunk - Uses H5Dread_chunk to read the data.\n");
    printf("              This bypasses the filter pipeline and can be\n");
    printf("              slightly more efficient (albeit dangerous).\n");
    printf("\n");
    printf("posixst - Uses pread(2) to read the data outside of the HDF5 library.\n");
    printf("          This is mainly a sanity check on the multithreaded algorithm.\n");
    printf("\n");
    printf("posixmt - Uses pread(2) to read the data outside of the HDF5 library\n");
    printf("          using multiple threads, the number of which can be set using\n");
    printf("          the -t parameter.\n");
    printf("\n");
    printf("Usage: reader [options] <filename> \n");
    printf("\n");
    printf("Options:\n");
    printf("\ta\tI/O algorithm (default|directchunk|posixst|posixmt)\n");
    printf("\nb\tShow thread bandwidth (default: no)\n");
    printf("\tn\tNumber of threads in thread pool (posixmt only, default is 4)\n");
    printf("\nt\tShow thread execution times (default: no)\n");
    printf("\t?\tPrint this help information\n");
    printf("\n");
} /* usage */

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

    struct timespec ts;
    struct timespec process_start_ts;
    struct timespec process_end_ts;

    int c;

    algorithm_e algorithm = HDF5_DEFAULT;

    int n_threads = 4;

    char *filename = NULL;

    while ((c = getopt(argc, argv, ":a:bn:t")) != -1) {
        switch (c) {
            case 'a':
                if (!strcmp(optarg, "directchunk"))
                    algorithm = DIRECT_CHUNK;
                else if (!strcmp(optarg, "posixst"))
                    algorithm = POSIX_ST;
                else if (!strcmp(optarg, "posixmt"))
                    algorithm = POSIX_MT;
                break;
            case 'b':
                show_thread_bandwidths_g = true;
                break;
            case 'n':
                n_threads = atoi(optarg);
                break;
            case 't':
                show_thread_times_g = true;
                break;
            case '?':
                usage();
                exit(EXIT_SUCCESS);
        }
    }

    /* File name is the last argument */
    if (optind != argc - 1) {
        printf("\n");
        printf("BADNESS: Data file name must be last parameter\n");
        printf("\n");
        usage();
        exit(EXIT_FAILURE);
    }
    else
        filename = argv[optind];

    printf("HDF5 multithreaded I/O work-around - reader\n");

    /* Spit out the clock resolutions */
    printf("\n");
    printf("Resolution of clocks as reported by clock_getres(3)\n");
    if (clock_getres(CLOCK_MONOTONIC, &ts) < 0)
        goto error;
    printf("%-32s", "CLOCK_MONOTONIC: ");
    printf("%lld.%.9ld s\n", (long long)ts.tv_sec, ts.tv_nsec);

    if (clock_getres(CLOCK_PROCESS_CPUTIME_ID, &ts) < 0)
        goto error;
    printf("%-32s", "CLOCK_PROCESS_CPUTIME_ID: ");
    printf("%lld.%.9ld s\n", (long long)ts.tv_sec, ts.tv_nsec);

    if (clock_getres(CLOCK_THREAD_CPUTIME_ID, &ts) < 0)
        goto error;
    printf("%-32s", "CLOCK_THREAD_CPUTIME_ID: ");
    printf("%lld.%.9ld s\n", (long long)ts.tv_sec, ts.tv_nsec);
    printf("\n");

    /***************************/
    /* Create/open HDF5 things */
    /***************************/

    /* START PROCESS TIMER */
    if (clock_gettime(CLOCK_MONOTONIC, &process_start_ts) < 0)
        goto error;

    if (H5I_INVALID_HID == (fid = H5Fopen(filename, H5F_ACC_RDONLY, H5P_DEFAULT)))
        goto error;

    if (H5I_INVALID_HID == (tid = H5Tcopy(H5T_NATIVE_UINT32)))
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
    if (HDF5_DEFAULT == algorithm)
        if (hdf5_default(did, tid, msid, fsid) < 0)
            goto error;

    /* H5Dread_chunk */
    if (DIRECT_CHUNK == algorithm)
        if (direct_chunk(did) < 0)
            goto error;

    /* Single-threaded version of multithreading work-around
     *
     * Mainly a sanity check on the algorithm if something goes wrong
     * with the multithreaded version.
     */
    if (POSIX_ST == algorithm)
        if (posix_single_thread(did, fsid, filename) < 0)
            goto error;

    /* Multithreading work-around */
    if (POSIX_MT == algorithm)
        if (posix_multithreaded(did, fsid, filename, n_threads) < 0)
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

    /* STOP PROCESS TIMER */
    if (clock_gettime(CLOCK_MONOTONIC, &process_end_ts) < 0)
        goto error;

    print_elapsed_sec(process_start_ts, process_end_ts);
    printf("\tProcess execution time (via CLOCK_MONOTONIC)\n");

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
