#include <stdlib.h>
#include <string.h>

#include "backend_filenames.h"

#define BACKEND_METADATA_TEMPLATE "metadata%u"
#define BACKEND_SNAPSHOT_TEMPLATE "snapshot-%llu-%llu-%llu"
#define BACKEND_SNAPSHOT_METADATA_TEMPLATE BACKEND_SNAPSHOT_TEMPLATE ".meta"
#define BACKEND_CLOSED_SEGMENT_TEMPLATE "%016llu-%016llu"
#define BACKEND_OPEN_SEGMENT_TEMPLATE "open-%llu"

#define BACKEND_FILENAME_SCAN(TYPE, N, ...)                                 \
    int consumed = 0;                                                       \
    int matched;                                                            \
    matched = sscanf(filename, BACKEND_##TYPE##_TEMPLATE "%n", __VA_ARGS__, \
                     &consumed);                                            \
    if (matched != N || consumed != (int)strlen(filename)) {                \
        return false;                                                       \
    }                                                                       \
    return true

static bool backendFilenameScanMetadata(const char *filename,
                                        struct BackendFilenameInfo *info)
{
    BACKEND_FILENAME_SCAN(METADATA, 1, &info->version);
}

static bool backendFilenameScanSnapshotMetadata(
    const char *filename,
    struct BackendFilenameInfo *info)
{
    BACKEND_FILENAME_SCAN(SNAPSHOT_METADATA, 3, &info->term, &info->index,
                          &info->timestamp);
}

static bool backendFilenameScanClosedSegment(const char *filename,
                                             struct BackendFilenameInfo *info)
{
    BACKEND_FILENAME_SCAN(CLOSED_SEGMENT, 2, &info->first_index,
                          &info->end_index);
}

static bool backendFilenameScanOpenSegment(const char *filename,
                                           struct BackendFilenameInfo *info)
{
    BACKEND_FILENAME_SCAN(OPEN_SEGMENT, 1, &info->counter);
}

void BackendScanFilename(const char *filename, struct BackendFilenameInfo *info)
{
#define X(TYPE, SCAN)                         \
    if (SCAN(filename, info)) {               \
        info->type = BACKEND_FILENAME_##TYPE; \
        return;                               \
    }

    X(METADATA, backendFilenameScanMetadata);
    X(SNAPSHOT_METADATA, backendFilenameScanSnapshotMetadata);
    X(CLOSED_SEGMENT, backendFilenameScanClosedSegment);
    X(OPEN_SEGMENT, backendFilenameScanOpenSegment);
#undef X

    info->type = BACKEND_FILENAME_UNKNOWN;
}

static int backendCompareFileInfos(const void *a, const void *b)
{
    const struct BackendFilenameInfo *info[2] = {
        (const struct BackendFilenameInfo *)a,
        (const struct BackendFilenameInfo *)b};

    if (info[0]->type != info[1]->type) {
        return info[0]->type < info[1]->type ? -1 : 1;
    }

    switch (info[0]->type) {
        case BACKEND_FILENAME_SNAPSHOT_METADATA:
            /* If term are different, the snaphot with the highest term is the
             * most recent. */
            if (info[0]->term != info[1]->term) {
                return info[0]->term > info[1]->term ? -1 : 1;
            }

            /* If the terms are identical and the index differ, the snapshot
             * with the highest index is the most recent */
            if (info[0]->index != info[1]->index) {
                return info[0]->index > info[1]->index ? -1 : 1;
            }

            /* If term and index are identical, compare the timestamp. */
            return info[0]->timestamp > info[1]->timestamp ? -1 : 1;

        case BACKEND_FILENAME_OPEN_SEGMENT:
            return info[0]->counter < info[1]->counter ? -1 : 1;

        default:
            break;
    }

    /* For the rest of the file types, alphabetical order works. */
    // return strcmp(name[0], name[1]);
    return 0;
}

void BackendSortFileInfos(struct BackendFilenameInfo infos[], unsigned n)
{
    qsort(infos, n, sizeof *infos, backendCompareFileInfos);
}
