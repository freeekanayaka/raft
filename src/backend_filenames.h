#ifndef BACKEND_FILENAMES_H_
#define BACKEND_FILENAMES_H_

#include "../include/raft/backend.h"

enum BackendFilenameType {
    BACKEND_FILENAME_METADATA = 1,
    BACKEND_FILENAME_SNAPSHOT_METADATA,
    BACKEND_FILENAME_SNAPSHOT,
    BACKEND_FILENAME_CLOSED_SEGMENT,
    BACKEND_FILENAME_OPEN_SEGMENT,
    BACKEND_FILENAME_UNKNOWN
};

struct BackendFilenameInfo
{
    enum BackendFilenameType type;
    union {
        struct /* BACKEND_FILENAME_METADATA */
        {
            unsigned version;
        };
        struct /* BACKEND_FILENAME_SNAPSHOT_METADATA */
        {
            raft_term term;
            raft_index index;
            unsigned long long timestamp;
        };
        struct /* BACKEND_FILENAME_CLOSED_SEGMENT */
        {
            raft_term first_index;
            raft_index end_index;
        };
        struct /* BACKEND_FILENAME_OPEN_SEGMENT */
        {
            unsigned long long counter;
        };
    };
};

void BackendScanFilename(const char *filename,
                         struct BackendFilenameInfo *info);

void BackendSortFileInfos(struct BackendFilenameInfo infos[], unsigned n);

#endif /* BACKEND_FILENAMES_H_ */
