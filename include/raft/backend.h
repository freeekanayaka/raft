#ifndef RAFT_BACKEND_H
#define RAFT_BACKEND_H

#include <stdint.h>

#include "../raft.h"

/* Current disk format version. */
#define RAFT_BACKEND_FORMAT_V1 1

#define RAFT_BACKEND_FILENAME_MAX_LEN 64

/* Format, version, term, vote */
#define RAFT_BACKEND_METADATA_SIZE (8 * 4)

enum raft_backend_load_io_type {
    RAFT_BACKEND_LOAD_READ,
    RAFT_BACKEND_LOAD_TRUNCATE_AND_RENAME,
    RAFT_BACKEND_LOAD_DELETE
};

struct raft_backend_load_io
{
    enum raft_backend_load_io_type type;
    char filename[RAFT_BACKEND_FILENAME_MAX_LEN];
    union {
        struct
        { /* BACKEND_LOAD_TRUNCATE_AND_RENAME */
            size_t size;
            const char *new_filename;
        };
    };
    void *queue[2];
};

struct raft_backend
{
    struct raft_tracer *tracer; /* Optional tracer implementation. */
    struct
    {
        raft_term term;
        raft_id voted_for;
        void *io[2];
    } load;
    struct
    {
        unsigned long long version; /* Current version */
        char
            filename[RAFT_BACKEND_FILENAME_MAX_LEN]; /* Name of file to write */
        uint8_t content[RAFT_BACKEND_METADATA_SIZE]; /* File content to write */
    } metadata;                                      /* Metadata file on disk */
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
};

RAFT_API int raft_backend_init(struct raft_backend *s);

RAFT_API void raft_backend_close(struct raft_backend *s);

RAFT_API int raft_backend_load_scan(struct raft_backend *s,
                                    const char *filenames[],
                                    unsigned n);

RAFT_API struct raft_backend_load_io *raft_backend_load_next(
    struct raft_backend *s);

RAFT_API int raft_backend_load_read(struct raft_backend *s,
                                    const char *filename,
                                    struct raft_buffer *buf);

RAFT_API int raft_backend_load_done(struct raft_backend *s,
                                    raft_term *term,
                                    raft_id *voted_for,
                                    struct raft_snapshot **snapshot,
                                    raft_index *start_index,
                                    struct raft_entry **entries,
                                    unsigned *n_entries);

RAFT_API int raft_backend_persist_term_and_vote(struct raft_backend *s,
                                                raft_term term,
                                                raft_id voted_for,
                                                const char **filename,
                                                struct raft_buffer *data);

RAFT_API const char *raft_backend_errmsg(struct raft_backend *s);

#endif /* RAFT_BACKEND_H */
