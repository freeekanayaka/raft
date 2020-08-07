#include "io.h"
#include "tracing.h"

#define tracef(...) Tracef(r->tracer, "  " __VA_ARGS__)

int ioPersistTermAndVote(struct raft *r, raft_term term, raft_id voted_for)
{
    struct raft_persist_term_and_vote *io;
    io = HeapMalloc(sizeof *io);
    if (io == NULL) {
        return RAFT_NOMEM;
    }

    io->type = RAFT_PERSIST_TERM_AND_VOTE;
    io->term = term;
    io->voted_for = voted_for;

    QUEUE_PUSH(&r->io, &io->queue);

    return 0;
}

int ioPersistEntries(struct raft *r,
                     raft_index first_index,
                     struct raft_entry *entries,
                     unsigned n)
{
    struct raft_persist_entries *io;
    io = HeapMalloc(sizeof *io);
    if (io == NULL) {
        return RAFT_NOMEM;
    }

    tracef("persist %u entries with first index %llu", n, first_index);

    io->type = RAFT_PERSIST_ENTRIES;
    io->first_index = first_index;
    io->entries = entries;
    io->n = n;

    QUEUE_PUSH(&r->io, &io->queue);

    return 0;
}

int ioPersistSnapshot(struct raft *r, struct raft_snapshot *snapshot)
{
    struct raft_persist_snapshot *io;
    io = HeapMalloc(sizeof *io);
    if (io == NULL) {
        return RAFT_NOMEM;
    }

    tracef("persist snapshot with last index %llu at term %llu",
           snapshot->index, snapshot->term);

    io->type = RAFT_PERSIST_SNAPSHOT;
    io->snapshot = *snapshot;

    QUEUE_PUSH(&r->io, &io->queue);

    return 0;
}

int ioSendMessage(struct raft *r,
                  raft_id id,
                  const char *address,
                  struct raft_message message)
{
    struct raft_send_message *io;
    io = HeapMalloc(sizeof *io);
    if (io == NULL) {
        return RAFT_NOMEM;
    }

    io->type = RAFT_SEND_MESSAGE;
    io->id = id;
    io->address = address;
    io->message = message;

    QUEUE_PUSH(&r->io, &io->queue);

    return 0;
}
