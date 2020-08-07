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

int raft_init(struct raft *r, const raft_id id)
{
    assert(r != NULL);
    r->id = id;
    r->clock = NULL;
    r->timeout = 0;
    r->tracer = NULL;
    /* Make a copy of the address */
    /* r->address = HeapMalloc(strlen(address) + 1);
    if (r->address == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    strcpy(r->address, address); */
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
    /* r->transfer = NULL; */
    /* r->snapshot.pending.term = 0; */
    r->snapshot.threshold = DEFAULT_SNAPSHOT_THRESHOLD;
    r->snapshot.trailing = DEFAULT_SNAPSHOT_TRAILING;
    r->snapshot.put.data = NULL;
    memset(r->errmsg, 0, sizeof r->errmsg);
    r->pre_vote = false;
    r->max_catch_up_rounds = DEFAULT_MAX_CATCH_UP_ROUNDS;
    r->max_catch_up_round_duration = DEFAULT_MAX_CATCH_UP_ROUND_DURATION;
    r->prng = 42;
    QUEUE_INIT(&r->io);
    return 0;
}

/*
static void ioCloseCb(struct raft_io *io)
{
    struct raft *r = io->data;
    raft_free(r->address);
    logClose(&r->log);
    raft_configuration_close(&r->configuration);
    if (r->close_cb != NULL) {
        r->close_cb(r);
    }
}
*/

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
}

void raft_seed(struct raft *r, unsigned n)
{
    r->prng = n;
}

int raft_start(struct raft *r,
               struct raft_clock *clock,
               raft_term term,
               raft_id voted_for,
               struct raft_snapshot_metadata *snapshot_metadata,
               raft_index start_index,
               struct raft_entry *entries,
               unsigned n_entries)
{
    int rv;

    r->clock = clock;

    rv = startRestore(r, term, voted_for, snapshot_metadata, start_index,
                      entries, n_entries);
    if (rv != 0) {
        return rv;
    }

    tracef("start - T:%llu V:%llu S:%llu/%llu L:%llu/%llu", term, voted_for,
           logSnapshotIndex(&r->log), logSnapshotTerm(&r->log),
           logLastIndex(&r->log) - logNumEntries(&r->log) + 1,
           logLastIndex(&r->log));

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
    }
}

int raft_done(struct raft *r, struct raft_io *io, int status)
{
    (void)r;
    assert(io != NULL);
    switch (io->type) {
        case RAFT_SEND_MESSAGE:
            doneSendMessage(r, (struct raft_send_message *)io, status);
            break;
        case RAFT_PERSIST_ENTRIES:
            break;
        case RAFT_PERSIST_TERM_AND_VOTE:
            assert(status == 0);
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
    if (r->transfer != NULL) {
        raft_time now = r->clock->now(r->clock);
        if (now - r->transfer->start >= r->election_timeout) {
            membershipLeadershipTransferClose(r);
        }
    }

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
        default:
            desc = "unknown";
            break;
    }
    tracef("recv %s from %llu", desc, id);
    return recvMessage(r, id, address, message);
}

RAFT_API int raft_accept(struct raft *r,
                         struct raft_buffer *commands,
                         unsigned n)
{
    tracef("accept %u commands starting at %lld", n, logLastIndex(&r->log) + 1);
    return clientAccept(r, commands, n);
}

int raft_state(struct raft *r)
{
    return r->state;
}

void raft_set_election_timeout(struct raft *r, const unsigned msecs)
{
    r->election_timeout = msecs;
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

/*
int raft_bootstrap(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    if (r->state != RAFT_UNAVAILABLE) {
        return RAFT_BUSY;
    }

    rv = r->io->bootstrap(r->io, conf);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_recover(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    if (r->state != RAFT_UNAVAILABLE) {
        return RAFT_BUSY;
    }

    rv = r->io->recover(r->io, conf);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

*/

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
