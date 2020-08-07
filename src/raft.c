#include "../include/raft.h"

#include <string.h>

#include "assert.h"
#include "byte.h"
#include "client.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "err.h"
#include "heap.h"
#include "log.h"
#include "membership.h"
#include "queue.h"
#include "recv.h"
#include "replication.h"
#include "start.h"
#include "tick.h"
#include "tracing.h"

#define DEFAULT_ELECTION_TIMEOUT 1000 /* One second */
#define DEFAULT_HEARTBEAT_TIMEOUT 100 /* One tenth of a second */
#define DEFAULT_SNAPSHOT_THRESHOLD 1024
#define DEFAULT_SNAPSHOT_TRAILING 2048

/* Number of milliseconds after which a server promotion will be aborted if the
 * server hasn't caught up with the logs yet. */
#define DEFAULT_MAX_CATCH_UP_ROUNDS 10
#define DEFAULT_MAX_CATCH_UP_ROUND_DURATION (5 * 1000)

#define tracef(...) Tracef(r->tracer, "> " __VA_ARGS__)

int raft_init(struct raft *r, const raft_id id, const char *address)
{
    assert(r != NULL);
    r->id = id;
    r->clock = NULL;
    r->tracer = NULL;
    /* Make a copy of the address */
    r->address = HeapMalloc(strlen(address) + 1);
    if (r->address == NULL) {
        return RAFT_NOMEM;
    }
    strcpy(r->address, address);
    r->current_term = 0;
    r->voted_for = 0;
    logInit(&r->log);
    configurationInit(&r->configuration);
    r->configuration_index = 0;
    r->configuration_uncommitted_index = 0;
    r->election_timeout = DEFAULT_ELECTION_TIMEOUT;
    r->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
    r->commit_index = 0;
    r->last_applied = 0;
    r->last_stored = 0;
    r->state = RAFT_UNAVAILABLE;
    r->transfer.id = 0;
    /* r->snapshot.pending.term = 0; */
    r->snapshot.threshold = DEFAULT_SNAPSHOT_THRESHOLD;
    r->snapshot.trailing = DEFAULT_SNAPSHOT_TRAILING;
    r->snapshot.data.base = NULL;
    r->snapshot.data.len = 0;
    /* r->snapshot.put.data = NULL; */
    memset(r->errmsg, 0, sizeof r->errmsg);
    r->pre_vote = false;
    r->max_catch_up_rounds = DEFAULT_MAX_CATCH_UP_ROUNDS;
    r->max_catch_up_round_duration = DEFAULT_MAX_CATCH_UP_ROUND_DURATION;
    r->prng = 42;
    QUEUE_INIT(&r->io);
    return 0;
}

void raft_close(struct raft *r)
{
    struct raft_io *io;
    if (r->state != RAFT_UNAVAILABLE) {
        convertToUnavailable(r);
    }
    while ((io = raft_pending(r))) {
        /* Possibly release log entries associated to pending I/O */
        struct raft_message *m;
        struct raft_persist_entries *p;
        struct raft_entry *entries = NULL;
        unsigned n;
        raft_index index;
        switch (io->type) {
            case RAFT_SEND_MESSAGE:
                m = &((struct raft_send_message *)io)->message;
                if (m->type == RAFT_IO_APPEND_ENTRIES) {
                    entries = m->append_entries.entries;
                    n = m->append_entries.n_entries;
                    index = m->append_entries.prev_log_index + 1;
                }
                break;
            case RAFT_PERSIST_ENTRIES:
                p = (struct raft_persist_entries *)io;
                entries = p->entries;
                n = p->n;
                index = p->first_index;
                break;
            default:
                break;
        };
        if (entries != NULL) {
            logRelease(&r->log, index, entries, n);
        }
        HeapFree(io);
    }
    configurationClose(&r->configuration);
    logClose(&r->log);
    HeapFree(r->address);
    if (r->snapshot.data.base != NULL) {
        HeapFree(r->snapshot.data.base);
    }
}

void raft_seed(struct raft *r, unsigned n)
{
    r->prng = n;
}

int raft_start(struct raft *r,
               struct raft_clock *clock,
               raft_term term,
               raft_id voted_for,
               struct raft_snapshot *snapshot,
               raft_index start_index,
               struct raft_entry *entries,
               unsigned n_entries)
{
    char msg[512];
    int rv;

    r->clock = clock;

    rv = startRestore(r, term, voted_for, snapshot, start_index, entries,
                      n_entries);
    if (rv != 0) {
        return rv;
    }

    sprintf(msg, "term %llu, vote %llu, ", term, voted_for);

    if (logSnapshotIndex(&r->log) > 0) {
        char msg_snapshot[64];
        sprintf(msg_snapshot, "snapshot %llu/%llu, ", logSnapshotIndex(&r->log),
                logSnapshotTerm(&r->log));
        strcat(msg, msg_snapshot);
    } else {
        strcat(msg, "no snapshot, ");
    }

    if (logNumEntries(&r->log)) {
        char msg_entries[64];
        raft_index first = logLastIndex(&r->log) - logNumEntries(&r->log) + 1;
        raft_index last = logLastIndex(&r->log);
        sprintf(msg_entries, "entries %llu/%llu to %llu/%llu", first,
                logTermOf(&r->log, first), last, logTermOf(&r->log, last));
        strcat(msg, msg_entries);
    } else {
        strcat(msg, "no entries");
    }

    tracef("%s", msg);

    rv = startConvert(r);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

struct raft_io *raft_pending(struct raft *r)
{
    struct raft_io *io;
    queue *head;

    if (QUEUE_IS_EMPTY(&r->io)) {
        return NULL;
    }

    head = QUEUE_HEAD(&r->io);
    io = QUEUE_DATA(head, struct raft_io, queue);
    QUEUE_REMOVE(head);

    return io;
}

static void doneSendMessage(struct raft *r,
                            struct raft_send_message *send,
                            int status)
{
    switch (send->message.type) {
        case RAFT_IO_APPEND_ENTRIES:
            replicationSendAppendEntriesDone(r, send, status);
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            replicationSendInstallSnapshotDone(r, send, status);
            break;
    }
}

static void donePersistEntries(struct raft *r,
                               struct raft_persist_entries *persist,
                               int status)
{
    tracef("done persist %u entries with first index %lld (status %d)",
           persist->n, persist->first_index, status);
    replicationPersistEntriesDone(r, persist, status);
}

static void donePersistSnapshot(struct raft *r,
                                struct raft_persist_snapshot *persist,
                                int status)
{
    tracef("done persist snapshot with last index %lld (status %d)",
           persist->snapshot.index, status);
    replicationPersistSnapshotDone(r, persist, status);
}

int raft_done(struct raft *r, struct raft_io *io, int status)
{
    assert(io != NULL);
    switch (io->type) {
        case RAFT_SEND_MESSAGE:
            doneSendMessage(r, (struct raft_send_message *)io, status);
            break;
        case RAFT_PERSIST_ENTRIES:
            donePersistEntries(r, (struct raft_persist_entries *)io, status);
            break;
        case RAFT_PERSIST_TERM_AND_VOTE:
            assert(status == 0);
            break;
        case RAFT_PERSIST_SNAPSHOT:
            donePersistSnapshot(r, (struct raft_persist_snapshot *)io, status);
            break;
    }
    HeapFree(io);
    return 0;
}

int raft_tick(struct raft *r)
{
    int rv = -1;

    tracef("tick");

    assert(r->state == RAFT_UNAVAILABLE || r->state == RAFT_FOLLOWER ||
           r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);

    switch (r->state) {
        case RAFT_FOLLOWER:
            rv = tickFollower(r);
            break;
        case RAFT_CANDIDATE:
            rv = tickCandidate(r);
            break;
        case RAFT_LEADER:
            rv = tickLeader(r);
            break;
    }

    if (rv != 0) {
        convertToUnavailable(r);
        return rv;
    }

    /* For all states: if there is a leadership transfer request in progress,
     * check if it's expired. */
    tickMaybeExpireTransfer(r);

    return 0;
}

int raft_recv(struct raft *r,
              raft_id id,
              const char *address,
              struct raft_message *message)
{
    const char *desc;
    switch (message->type) {
        case RAFT_IO_REQUEST_VOTE:
            desc = "request vote";
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            desc = "request vote result";
            break;
        case RAFT_IO_APPEND_ENTRIES:
            desc = "append entries";
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            desc = "append entries result";
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            desc = "install snapshot";
            break;
        case RAFT_IO_TIMEOUT_NOW:
            desc = "timeout now";
            break;
        default:
            desc = "unknown message";
            break;
    }
    tracef("recv %s from server %llu", desc, id);
    return recvMessage(r, id, address, message);
}

int raft_accept(struct raft *r, struct raft_entry *entries, unsigned n)
{
    tracef("accept %u commands starting at %lld", n, logLastIndex(&r->log) + 1);
    return clientAccept(r, entries, n);
}

int raft_snapshot(struct raft *r, raft_index index)
{
    tracef("take snapshot with last index %llu, leave %u trailing entries",
           index, r->snapshot.trailing);
    return clientSnapshot(r, index);
}

int raft_transfer(struct raft *r, raft_id id)
{
    tracef("transfer leadership to %llu", id);
    return clientTransfer(r, id);
}

int raft_state(struct raft *r)
{
    return r->state;
}

void raft_set_election_timeout(struct raft *r, const unsigned msecs)
{
    r->election_timeout = msecs;
    switch (r->state) {
        case RAFT_FOLLOWER:
        case RAFT_CANDIDATE:
            electionUpdateRandomizedTimeout(r);
            break;
    }
}

void raft_set_heartbeat_timeout(struct raft *r, const unsigned msecs)
{
    r->heartbeat_timeout = msecs;
}

void raft_set_snapshot_threshold(struct raft *r, unsigned n)
{
    r->snapshot.threshold = n;
}

void raft_set_snapshot_trailing(struct raft *r, unsigned n)
{
    r->snapshot.trailing = n;
}

void raft_set_max_catch_up_rounds(struct raft *r, unsigned n)
{
    r->max_catch_up_rounds = n;
}

void raft_set_max_catch_up_round_duration(struct raft *r, unsigned msecs)
{
    r->max_catch_up_round_duration = msecs;
}

void raft_set_pre_vote(struct raft *r, bool enabled)
{
    r->pre_vote = enabled;
}

const char *raft_errmsg(struct raft *r)
{
    return r->errmsg;
}

const char *raft_strerror(int errnum)
{
    return errCodeToString(errnum);
}

void raft_configuration_init(struct raft_configuration *c)
{
    configurationInit(c);
}

void raft_configuration_close(struct raft_configuration *c)
{
    configurationClose(c);
}

int raft_configuration_add(struct raft_configuration *c,
                           const raft_id id,
                           const char *address,
                           const int role)
{
    return configurationAdd(c, id, address, role);
}

int raft_configuration_encode(const struct raft_configuration *c,
                              struct raft_buffer *buf)
{
    return configurationEncode(c, buf);
}

unsigned long long raft_digest(const char *text, unsigned long long n)
{
    struct byteSha1 sha1;
    uint8_t value[20];
    uint64_t n64 = byteFlip64((uint64_t)n);
    uint64_t digest;

    byteSha1Init(&sha1);
    byteSha1Update(&sha1, (const uint8_t *)text, (uint32_t)strlen(text));
    byteSha1Update(&sha1, (const uint8_t *)&n64, (uint32_t)(sizeof n64));
    byteSha1Digest(&sha1, value);

    memcpy(&digest, value + (sizeof value - sizeof digest), sizeof digest);

    return byteFlip64(digest);
}

#undef tracef
