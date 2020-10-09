#include <string.h>

#include "../include/raft/backend.h"

#include "assert.h"
#include "backend_filenames.h"
#include "backend_metadata.h"
#include "backend_snapshot.h"
#include "heap.h"
#include "queue.h"
#include "tracing.h"

#define tracef(...) Tracef(s->tracer, "> " __VA_ARGS__)

int raft_backend_init(struct raft_backend *s)
{
    s->tracer = NULL;
    s->load.term = 0;
    s->load.voted_for = 0;
    QUEUE_INIT(&s->load.io);
    s->metadata.version = 0;
    memset(s->errmsg, 0, sizeof s->errmsg);
    return 0;
}

static void backendCloseLoadIo(struct raft_backend *s)
{
    struct raft_backend_load_io *io;
    while ((io = raft_backend_load_next(s))) {
        HeapFree(io);
    }
}

void raft_backend_close(struct raft_backend *s)
{
    backendCloseLoadIo(s);
}

int raft_backend_load_scan(struct raft_backend *s,
                           const char *filenames[],
                           unsigned n)
{
    struct BackendFilenameInfo *infos;
    struct raft_backend_load_io *io;
    unsigned i;
    int rv;

    if (n == 0) {
        return 0;
    }

    infos = HeapMalloc(n * sizeof *infos);
    if (infos == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    for (i = 0; i < n; i++) {
        tracef("scan %s", filenames[i]);
        BackendScanFilename(filenames[i], &infos[i]);
    }

    BackendSortFileInfos(infos, n);

    for (i = 0; i < n; i++) {
        struct BackendFilenameInfo *info = &infos[i];
        switch (info->type) {
            case BACKEND_FILENAME_METADATA:
                io = HeapMalloc(sizeof *io);
                if (io == NULL) {
                    return RAFT_NOMEM;
                }
                io->type = RAFT_BACKEND_LOAD_READ;
                strcpy(io->filename, "metadata1");
                QUEUE_PUSH(&s->load.io, &io->queue);
                break;

            default:
                break;
        }
    }

    HeapFree(infos);

    return 0;

err:
    assert(rv != 0);
    return rv;
}

RAFT_API struct raft_backend_load_io *raft_backend_load_next(
    struct raft_backend *s)
{
    struct raft_backend_load_io *io;
    queue *head;

    if (QUEUE_IS_EMPTY(&s->load.io)) {
        return NULL;
    }

    head = QUEUE_HEAD(&s->load.io);
    io = QUEUE_DATA(head, struct raft_backend_load_io, queue);
    QUEUE_REMOVE(head);

    return io;
}

int raft_backend_load_read(struct raft_backend *s,
                           const char *filename,
                           struct raft_buffer *buf)
{
    struct BackendFilenameInfo info;
    tracef("read %s", filename);

    BackendScanFilename(filename, &info);

    switch (info.type) {
        case BACKEND_FILENAME_METADATA:
            s->load.term = 1;
            HeapFree(buf->base);
            break;
        default:
            break;
    }

    return 0;
}

int raft_backend_load_done(struct raft_backend *s,
                           raft_term *term,
                           raft_id *voted_for,
                           struct raft_snapshot **snapshot,
                           raft_index *start_index,
                           struct raft_entry **entries,
                           unsigned *n_entries)
{
    *term = s->load.term;
    *voted_for = s->load.voted_for;
    *snapshot = NULL;
    *start_index = 1;
    *entries = NULL;
    *n_entries = 0;
    return 0;
}

int raft_backend_persist_term_and_vote(struct raft_backend *s,
                                       raft_term term,
                                       raft_id voted_for,
                                       const char **filename,
                                       struct raft_buffer *data)
{
    BackendMetadataEncode(s, term, voted_for);
    *filename = s->metadata.filename;
    data->base = s->metadata.content;
    data->len = sizeof s->metadata.content;
    return 0;
}

const char *raft_backend_errmsg(struct raft_backend *s)
{
    return s->errmsg;
}

#undef tracef
