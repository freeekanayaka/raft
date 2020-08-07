#include "../../src/queue.h"

#include "cluster.h"
#include "munit.h"

/* Defaults */
#define DEFAULT_NETWORK_LATENCY 10
#define DEFAULT_DISK_LATENCY 10

enum operation_type { OPERATION_IO = 0, OPERATION_TRANSMIT };

/* Track pending async operations. */
struct operation
{
    enum operation_type type; /* Either OPERATION_IO or OPERATION_TRANSMIT */
    raft_id id;               /* Target server ID. */
    raft_time completion;     /* When the operation should complete */
    union {
        struct raft_io *io;
        struct
        {
            raft_id from;
            struct raft_message message;
        } transmit;
    };
    queue queue;
};

/* Mark @id1 as disconnected from @id2. */
struct disconnect
{
    raft_id id1;
    raft_id id2;
    queue queue;
};

/* Initialize an empty disk with no persisted data. */
static void diskInit(struct test_disk *d)
{
    d->term = 0;
    d->voted_for = 0;
    d->snapshot = NULL;
    d->start_index = 1;
    d->entries = NULL;
    d->n_entries = 0;
}

/* Release all memory used by the disk snapshot, if present. */
static void diskDestroySnapshotIfPresent(struct test_disk *d)
{
    if (d->snapshot == NULL) {
        return;
    }
    raft_configuration_close(&d->snapshot->configuration);
    free(d->snapshot->data.base);
    raft_free(d->snapshot);
    d->snapshot = NULL;
}

/* Release all memory used by the disk. */
static void diskClose(struct test_disk *d)
{
    unsigned i;

    for (i = 0; i < d->n_entries; i++) {
        free(d->entries[i].buf.base);
    }
    free(d->entries);
    diskDestroySnapshotIfPresent(d);
}

static void entryCopy(const struct raft_entry *src, struct raft_entry *dst)
{
    dst->term = src->term;
    dst->type = src->type;
    dst->buf.len = src->buf.len;
    dst->buf.base = munit_malloc(dst->buf.len);
    memcpy(dst->buf.base, src->buf.base, dst->buf.len);
    dst->batch = NULL;
}

/* Append a new entry to the log. */
static void diskPersistEntry(struct test_disk *d, struct raft_entry *entry)
{
    d->n_entries++;
    d->entries = realloc(d->entries, d->n_entries * sizeof *d->entries);
    munit_assert_ptr_not_null(d->entries);
    entryCopy(entry, &d->entries[d->n_entries - 1]);
}

/* Set the persisted term. */
static void diskPersistTerm(struct test_disk *d, raft_term term)
{
    d->term = term;
}

/* Set the persisted vote. */
static void diskPersistVote(struct test_disk *d, raft_id voted_for)
{
    d->voted_for = voted_for;
}

static void confCopy(const struct raft_configuration *src,
                     struct raft_configuration *dst)
{
    unsigned i;
    int rv;

    raft_configuration_init(dst);
    for (i = 0; i < src->n; i++) {
        struct raft_server *server = &src->servers[i];
        rv = raft_configuration_add(dst, server->id, server->address,
                                    server->role);
        munit_assert_int(rv, ==, 0);
    }
}

static void snapshotCopy(const struct raft_snapshot *src,
                         struct raft_snapshot *dst)
{
    dst->index = src->index;
    dst->term = src->term;

    confCopy(&src->configuration, &dst->configuration);
    dst->configuration_index = src->configuration_index;

    dst->data.len = src->data.len;
    dst->data.base = raft_malloc(dst->data.len);
    munit_assert_ptr_not_null(dst->data.base);
    memcpy(dst->data.base, src->data.base, src->data.len);
}

/* Set the persisted snapshot. */
static void diskPersistSnapshot(struct test_disk *d,
                                struct raft_snapshot *snapshot)
{
    diskDestroySnapshotIfPresent(d);
    d->snapshot = munit_malloc(sizeof *d->snapshot);
    snapshotCopy(snapshot, d->snapshot);

    /* If there are no entries, set the start index to the snapshot's last
     * index. */
    if (d->n_entries == 0) {
        d->start_index = snapshot->index + 1;
    }
}

/* Load the latest snapshot. */
static void diskLoadSnapshot(struct test_disk *d,
                             struct raft_snapshot *snapshot)
{
    munit_assert_ptr_not_null(d->snapshot);
    snapshotCopy(d->snapshot, snapshot);
}

/* Truncate all entries from the given index onwards. If there are no entries at
 * the given index, this is a no-op. */
static void diskTruncateEntries(struct test_disk *d, raft_index index)
{
    unsigned i;

    if (index == d->start_index + d->n_entries) {
        return;
    }

    munit_assert_int(index, >=, d->start_index);
    munit_assert_int(index, <=, d->start_index + d->n_entries);

    for (i = index - d->start_index; i < d->n_entries; i++) {
        free(d->entries[i].buf.base);
        d->n_entries--;
    }
}

/* Load all data persisted on the disk. */
static void diskLoad(struct test_disk *d,
                     raft_term *term,
                     raft_id *voted_for,
                     struct raft_snapshot **snapshot,
                     raft_index *start_index,
                     struct raft_entry **entries,
                     unsigned *n_entries)
{
    size_t size = 0;
    void *batch;
    uint8_t *cursor;
    unsigned i;

    *term = d->term;
    *voted_for = d->voted_for;
    if (d->snapshot != NULL) {
        *snapshot = raft_malloc(sizeof **snapshot);
        munit_assert_ptr_not_null(*snapshot);
        diskLoadSnapshot(d, *snapshot);
    } else {
        *snapshot = NULL;
    }
    *start_index = d->start_index;
    *n_entries = d->n_entries;

    if (*n_entries == 0) {
        *entries = NULL;
        return;
    }

    /* Calculate the total size of the entries content and allocate the
     * batch. */
    for (i = 0; i < d->n_entries; i++) {
        size += d->entries[i].buf.len;
    }

    batch = raft_malloc(size);
    munit_assert_ptr_not_null(batch);

    /* Copy the entries. */
    *entries = raft_malloc(d->n_entries * sizeof **entries);
    munit_assert_ptr_not_null(*entries);

    cursor = batch;

    for (i = 0; i < d->n_entries; i++) {
        (*entries)[i].term = d->entries[i].term;
        (*entries)[i].type = d->entries[i].type;
        (*entries)[i].buf.base = cursor;
        (*entries)[i].buf.len = d->entries[i].buf.len;
        (*entries)[i].batch = batch;
        memcpy((*entries)[i].buf.base, d->entries[i].buf.base,
               d->entries[i].buf.len);
        cursor += d->entries[i].buf.len;
    }
}

/* Custom emit tracer function which includes the server ID. */
static void serverEmit(struct raft_tracer *t,
                       const char *file,
                       int line,
                       const char *message)
{
    struct test_server *server;
    struct test_cluster *cluster;
    char trace[1024];
    (void)file;
    (void)line;
    server = t->data;
    cluster = server->cluster;
    if (message[0] == '>') {
        snprintf(trace, sizeof trace, "[%4lld] %llu %s", cluster->time,
                 server->raft.id, message);
    } else {
        snprintf(trace, sizeof trace, "         %s", message);
    }
    strcat(cluster->trace, trace);
    strcat(cluster->trace, "\n");
    fprintf(stderr, "%s\n", trace);
}

static raft_time serverClockNow(struct raft_clock *c)
{
    struct test_server *server = c->data;
    return server->cluster->time;
}

static void serverClockTimeout(struct raft_clock *c, unsigned msecs)
{
    struct test_server *server = c->data;
    server->timeout = server->cluster->time + msecs;
}

/* Initialize a new server object. */
static void serverInit(struct test_server *s,
                       raft_id id,
                       struct test_cluster *cluster)
{
    char address[64];
    unsigned seed;
    int rv;

    diskInit(&s->disk);

    s->tracer.data = s;
    s->tracer.emit = serverEmit;

    s->clock.data = s;
    s->clock.now = serverClockNow;
    s->clock.timeout = serverClockTimeout;

    s->timeout = 0;

    sprintf(address, "%llu", id);

    rv = raft_init(&s->raft, id, address);
    munit_assert_int(rv, ==, 0);

    s->raft.tracer = &s->tracer;

    switch (id) {
        case 1:
            seed = 13;
            break;
        case 2:
            seed = 275;
            break;
        case 3:
            seed = 265;
            break;
        default:
            seed = id;
            break;
    }

    raft_seed(&s->raft, seed);
    raft_set_election_timeout(&s->raft, 100);
    raft_set_heartbeat_timeout(&s->raft, 40);

    s->cluster = cluster;
    s->network_latency = DEFAULT_NETWORK_LATENCY;
    s->disk_latency = DEFAULT_DISK_LATENCY;
    s->running = false;
}

/* Reset the state of the given server. */
static void serverKill(struct test_server *s)
{
    char address[64];
    int rv;
    sprintf(address, "%llu", s->raft.id);
    raft_close(&s->raft);
    rv = raft_init(&s->raft, s->raft.id, address);
    munit_assert_int(rv, ==, 0);
    s->running = false;
}

/* Release all resources used by a server object. */
static void serverClose(struct test_server *s)
{
    diskClose(&s->disk);
    raft_close(&s->raft);
}

/* Start the server by invoking raft_start() with the current disk state and
 * scheduling the next raft_tick() call. */
static void serverStart(struct test_server *s)
{
    raft_term term;
    raft_id voted_for;
    struct raft_snapshot *snapshot;
    raft_index start_index;
    struct raft_entry *entries;
    unsigned n_entries;
    int rv;

    diskLoad(&s->disk, &term, &voted_for, &snapshot, &start_index, &entries,
             &n_entries);

    rv = raft_start(&s->raft, &s->clock, term, voted_for, snapshot, start_index,
                    entries, n_entries);
    munit_assert_int(rv, ==, 0);

    s->running = true;
}

static void serverHandleSendMessage(struct test_server *s, struct raft_io *io)
{
    struct operation *operation = munit_malloc(sizeof *operation);

    operation->type = OPERATION_IO;
    operation->id = s->raft.id;

    /* TODO: simulate the presence of an OS send buffer, whose available size
     * might delay the completion of send requests */
    operation->completion = s->cluster->time;

    operation->io = io;

    QUEUE_PUSH(&s->cluster->operations, &operation->queue);
}

static void serverHandlePersistEntries(struct test_server *s,
                                       struct raft_io *io)
{
    struct operation *operation = munit_malloc(sizeof *operation);

    operation->type = OPERATION_IO;
    operation->id = s->raft.id;

    operation->completion = s->cluster->time + s->disk_latency;

    operation->io = io;

    QUEUE_PUSH(&s->cluster->operations, &operation->queue);
}

static void serverHandlePersistTermAndVote(struct test_server *s,
                                           struct raft_io *io)
{
    struct raft_persist_term_and_vote *persist =
        (struct raft_persist_term_and_vote *)io;
    int rv;
    diskPersistTerm(&s->disk, persist->term);
    diskPersistVote(&s->disk, persist->voted_for);
    rv = raft_done(&s->raft, io, 0);
    munit_assert_int(rv, ==, 0);
}

static void serverHandlePersistSnapshot(struct test_server *s,
                                        struct raft_io *io)
{
    struct operation *operation = munit_malloc(sizeof *operation);

    operation->type = OPERATION_IO;
    operation->id = s->raft.id;

    operation->completion = s->cluster->time + s->disk_latency;

    operation->io = io;

    QUEUE_PUSH(&s->cluster->operations, &operation->queue);
}

/* Pull all messages currently pending in this server's #io queue, and transfer
 * them to the cluster I/O queue. */
static void serverHandleIo(struct test_server *s)
{
    struct raft_io *io;

    if (!s->running) {
        return;
    }

    while ((io = raft_pending(&s->raft))) {
        switch (io->type) {
            case RAFT_SEND_MESSAGE:
                serverHandleSendMessage(s, io);
                break;
            case RAFT_PERSIST_ENTRIES:
                serverHandlePersistEntries(s, io);
                break;
            case RAFT_PERSIST_TERM_AND_VOTE:
                serverHandlePersistTermAndVote(s, io);
                break;
            case RAFT_PERSIST_SNAPSHOT:
                serverHandlePersistSnapshot(s, io);
                break;
        }
    }
}

/* Invoke raft_tick(). */
static int serverTick(struct test_server *s)
{
    s->cluster->time = s->timeout;
    return raft_tick(&s->raft);
}

/* Create a single batch of entries containing a copy of the given entries,
 * including their data. Use raft_malloc() since memory ownership is going to be
 * handed over to raft via raft_recv(). */
static void copyEntries(const struct raft_entry *src,
                        struct raft_entry **dst,
                        const size_t n)
{
    size_t size = 0;
    void *batch;
    uint8_t *cursor;
    unsigned i;

    if (n == 0) {
        *dst = NULL;
        return;
    }

    /* Calculate the total size of the entries content and allocate the
     * batch. */
    for (i = 0; i < n; i++) {
        size += src[i].buf.len;
    }

    batch = raft_malloc(size);
    munit_assert_ptr_not_null(batch);

    /* Copy the entries. */
    *dst = raft_malloc(n * sizeof **dst);
    munit_assert_ptr_not_null(*dst);

    cursor = batch;

    for (i = 0; i < n; i++) {
        (*dst)[i].term = src[i].term;
        (*dst)[i].type = src[i].type;
        (*dst)[i].buf.base = cursor;
        (*dst)[i].buf.len = src[i].buf.len;
        (*dst)[i].batch = batch;
        memcpy((*dst)[i].buf.base, src[i].buf.base, src[i].buf.len);
        cursor += src[i].buf.len;
    }
}

static int serverCompleteSendMessage(struct test_server *s,
                                     struct raft_send_message *send)
{
    struct operation *operation;
    queue *head;

    /* Check if there's a disconnection. */
    QUEUE_FOREACH(head, &s->cluster->disconnect)
    {
        struct disconnect *d = QUEUE_DATA(head, struct disconnect, queue);
        if (d->id1 == s->raft.id && d->id2 == send->id) {
            return RAFT_NOCONNECTION;
        }
    }

    operation = munit_malloc(sizeof *operation);

    operation->type = OPERATION_TRANSMIT;
    operation->id = send->id;

    operation->completion = s->cluster->time + s->network_latency;
    operation->transmit.from = s->raft.id;
    operation->transmit.message = send->message;

    switch (send->message.type) {
        case RAFT_IO_APPEND_ENTRIES:
            /* Create a copy of the entries being sent, so the memory of the
             * original message can be released when raft_done() is called. */
            copyEntries(send->message.append_entries.entries,
                        &operation->transmit.message.append_entries.entries,
                        send->message.append_entries.n_entries);
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            /* Create a copy of the snapshot being sent, so the memory of the
             * original message can be released when raft_done() is called. */
            {
                struct raft_snapshot *src = s->disk.snapshot;
                struct raft_install_snapshot *dst =
                    &operation->transmit.message.install_snapshot;
                dst->last_index = src->index;
                dst->last_term = src->term;
                confCopy(&src->configuration, &dst->conf);
                dst->conf_index = src->configuration_index;
                dst->data.len = src->data.len;
                dst->data.base = raft_malloc(dst->data.len);
                munit_assert_ptr_not_null(dst->data.base);
                memcpy(dst->data.base, src->data.base, src->data.len);
            }
            break;
    }

    QUEUE_PUSH(&s->cluster->operations, &operation->queue);

    return 0;
}

static int serverCompletePersistEntries(struct test_server *s,
                                        struct raft_persist_entries *persist)
{
    unsigned i;

    /* Possibly truncate stale entries. */
    diskTruncateEntries(&s->disk, persist->first_index);

    for (i = 0; i < persist->n; i++) {
        diskPersistEntry(&s->disk, &persist->entries[i]);
    }

    return 0;
}

static int serverCompletePersistSnapshot(struct test_server *s,
                                         struct raft_persist_snapshot *persist)
{
    /* Erase the current log. */
    diskTruncateEntries(&s->disk, s->disk.start_index);

    /* Install the given snapshot. */
    diskPersistSnapshot(&s->disk, &persist->snapshot);

    return 0;
}

static int serverCompleteIo(struct test_server *s, struct operation *operation)
{
    struct raft_io *io = operation->io;
    int status = 0;

    switch (io->type) {
        case RAFT_SEND_MESSAGE:
            status =
                serverCompleteSendMessage(s, (struct raft_send_message *)io);
            break;
        case RAFT_PERSIST_ENTRIES:
            status = serverCompletePersistEntries(
                s, (struct raft_persist_entries *)io);
            break;
        case RAFT_PERSIST_SNAPSHOT:
            status = serverCompletePersistSnapshot(
                s, (struct raft_persist_snapshot *)io);
            break;
        case RAFT_PERSIST_TERM_AND_VOTE:
            munit_error("unexpected persist term and vote request");
            break;
    };

    return raft_done(&s->raft, io, status);
}

/* Release the memory used by a transmit operation. */
static void clearTransmitOperation(struct operation *o)
{
    switch (o->transmit.message.type) {
        case RAFT_IO_APPEND_ENTRIES:
            if (o->transmit.message.append_entries.n_entries > 0) {
                struct raft_entry *entries =
                    o->transmit.message.append_entries.entries;
                raft_free(entries[0].buf.base);
                raft_free(entries);
            }
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            raft_configuration_close(
                &o->transmit.message.install_snapshot.conf);
            raft_free(o->transmit.message.install_snapshot.data.base);
            break;
    }
}

static int serverCompleteTransmit(struct test_server *s,
                                  struct operation *operation)
{
    char address[64];
    queue *head;

    if (!s->running) {
        clearTransmitOperation(operation);
        return 0;
    }

    /* Check if there's a disconnection. */
    QUEUE_FOREACH(head, &s->cluster->disconnect)
    {
        struct disconnect *d = QUEUE_DATA(head, struct disconnect, queue);
        if (d->id1 == operation->transmit.from && d->id2 == s->raft.id) {
            clearTransmitOperation(operation);
            return 0;
        }
    }

    sprintf(address, "%llu", operation->transmit.from);
    return raft_recv(&s->raft, operation->transmit.from, address,
                     &operation->transmit.message);
}

/* Complete either a @raft_io or transmit operation. */
static int serverCompleteOperation(struct test_server *s,
                                   struct operation *operation)
{
    int rv;
    s->cluster->time = operation->completion;
    QUEUE_REMOVE(&operation->queue);
    switch (operation->type) {
        case OPERATION_IO:
            rv = serverCompleteIo(s, operation);
            break;
        case OPERATION_TRANSMIT:
            rv = serverCompleteTransmit(s, operation);
            break;
        default:
            rv = -1;
            munit_errorf("unexpected operation type %d", operation->type);
            break;
    }
    free(operation);
    return rv;
}

/* Return the server with the given @id. */
static struct test_server *clusterGetServer(struct test_cluster *c, raft_id id)
{
    munit_assert_int(id, <=, c->n);
    return c->servers[id - 1];
}

void test_cluster_setup(const MunitParameter params[], struct test_cluster *c)
{
    (void)params;
    c->time = 0;
    c->servers = NULL;
    c->n = 0;
    QUEUE_INIT(&c->operations);
    QUEUE_INIT(&c->disconnect);
    c->status = 0;
}

static void serverCancelIo(struct test_server *s, struct operation *operation)
{
    struct raft_io *io = operation->io;
    int rv;

    rv = raft_done(&s->raft, io, RAFT_CANCELED);
    munit_assert_int(rv, ==, 0);
}

void test_cluster_tear_down(struct test_cluster *c)
{
    unsigned i;

    /* Drop pending operations */
    while (!QUEUE_IS_EMPTY(&c->operations)) {
        struct test_server *server;
        struct operation *operation;
        queue *head;
        head = QUEUE_HEAD(&c->operations);
        operation = QUEUE_DATA(head, struct operation, queue);
        switch (operation->type) {
            case OPERATION_IO:
                server = clusterGetServer(c, operation->id);
                serverCancelIo(server, operation);
                break;
            case OPERATION_TRANSMIT:
                clearTransmitOperation(operation);
            default:
                break;
        }
        QUEUE_REMOVE(&operation->queue);
        free(operation);
    }

    /* Drop pending disconnects */
    while (!QUEUE_IS_EMPTY(&c->disconnect)) {
        struct disconnect *disconnect;
        queue *head;
        head = QUEUE_HEAD(&c->disconnect);
        disconnect = QUEUE_DATA(head, struct disconnect, queue);
        QUEUE_REMOVE(&disconnect->queue);
        free(disconnect);
    }

    for (i = 0; i < c->n; i++) {
        serverClose(c->servers[i]);
        free(c->servers[i]);
    }

    free(c->servers);
}

raft_time test_cluster_now(struct test_cluster *c)
{
    return c->time;
}

bool test_cluster_trace(struct test_cluster *c, const char *expected)
{
    size_t n1;
    size_t n2;
    size_t match = 0; /* End of last matching line. */
    size_t i;

consume:
    n1 = strlen(c->trace);
    n2 = strlen(expected);

    for (i = 0; i < n1 && i < n2; i++) {
        if (c->trace[i] == expected[i]) {
            if (c->trace[i] == '\n') {
                match = i;
            }
            continue;
        }
        break;
    }

    /* Check if we produced more output than the expected one. */
    if (n1 > n2) {
        return false;
    }

    /* If there's more expected output, check that so far we're good, then step
     * and repeat. */
    if (n1 < n2) {
        if (i != n1) {
            goto mismatch;
        }
        c->trace[0] = 0;
        expected += i;
        test_cluster_step(c);
        goto consume;
    }

    munit_assert_int(n1, ==, n2);
    if (i != n1) {
        goto mismatch;
    }

    c->trace[0] = 0;

    return true;

mismatch:
    fprintf(stderr, "==> Expected:\n");
    fprintf(stderr, "%s\n", expected);

    fprintf(stderr, "==> Actual:\n");
    fprintf(stderr, "%s\n", c->trace);

    return false;
}

void test_cluster_grow(struct test_cluster *c, unsigned n)
{
    unsigned i;

    c->servers = realloc(c->servers, (c->n + n) * sizeof *c->servers);
    munit_assert_ptr_not_null(c->servers);

    for (i = c->n; i < c->n + n; i++) {
        raft_id id = i + 1;
        c->servers[i] = munit_malloc(sizeof *c->servers[i]);
        serverInit(c->servers[i], id, c);
    }

    c->n += n;
}

struct raft *test_cluster_get(struct test_cluster *c, raft_id id)
{
    struct test_server *server = clusterGetServer(c, id);
    return &server->raft;
}

void test_cluster_persist_entry(struct test_cluster *c,
                                raft_id id,
                                struct raft_entry *entry)
{
    struct test_server *server = clusterGetServer(c, id);
    diskPersistEntry(&server->disk, entry);
}

void test_cluster_persist_term(struct test_cluster *c,
                               raft_id id,
                               raft_term term)
{
    struct test_server *server = clusterGetServer(c, id);
    diskPersistTerm(&server->disk, term);
}

void test_cluster_persist_snapshot(struct test_cluster *c,
                                   raft_id id,
                                   struct raft_snapshot *snapshot)
{
    struct test_server *server = clusterGetServer(c, id);
    diskPersistSnapshot(&server->disk, snapshot);
}

void test_cluster_start(struct test_cluster *c, raft_id id)
{
    struct test_server *server = clusterGetServer(c, id);
    serverStart(server);
}

/* Pull all messages currently pending in the #io queues of all
 * running servers and push them to our internal I/O queue. */
static void clusterHandleIo(struct test_cluster *c)
{
    unsigned i;
    for (i = 0; i < c->n; i++) {
        struct test_server *server = c->servers[i];
        serverHandleIo(server);
    }
}

/* Return the server with the earliest raft->timeout value. */
static struct test_server *clusterGetServerWithEarliestTimeout(
    struct test_cluster *c)
{
    struct test_server *server = NULL;
    unsigned j;
    for (j = 0; j < c->n; j++) {
        struct test_server *other = c->servers[j];
        if (!other->running) {
            continue;
        }
        if (server == NULL || other->timeout < server->timeout) {
            server = other;
        }
    }
    return server;
}

/* Return the operation with the lowest completion time. */
static struct operation *clusterGetOperationWithEarliestCompletion(
    struct test_cluster *c)
{
    struct operation *operation = NULL;
    queue *head;
    QUEUE_FOREACH(head, &c->operations)
    {
        struct operation *other = QUEUE_DATA(head, struct operation, queue);
        if (operation == NULL || other->completion < operation->completion) {
            operation = other;
        }
    }

    return operation;
}

void test_cluster_step(struct test_cluster *c)
{
    struct test_server *server;
    struct operation *operation;
    int rv;

    clusterHandleIo(c);
    server = clusterGetServerWithEarliestTimeout(c);
    operation = clusterGetOperationWithEarliestCompletion(c);

    if (operation == NULL || server->timeout < operation->completion) {
        rv = serverTick(server);
    } else {
        server = clusterGetServer(c, operation->id);
        rv = serverCompleteOperation(server, operation);
    }
    munit_assert_int(rv, ==, c->status);
}

void test_cluster_fail(struct test_cluster *c, int status)
{
    c->status = status;
    test_cluster_step(c);
    c->status = 0;
}

void test_cluster_elapse(struct test_cluster *c, unsigned msecs)
{
    struct test_server *server;
    struct operation *operation;
    raft_time time = c->time + msecs;

    clusterHandleIo(c);

    server = clusterGetServerWithEarliestTimeout(c);
    munit_assert_int(time, <, server->timeout);

    operation = clusterGetOperationWithEarliestCompletion(c);
    if (operation != NULL) {
        munit_assert_int(time, <=, operation->completion);
    }

    c->time = time;
}

void test_cluster_disconnect(struct test_cluster *c, raft_id id1, raft_id id2)
{
    struct disconnect *disconnect = munit_malloc(sizeof *disconnect);
    disconnect->id1 = id1;
    disconnect->id2 = id2;
    QUEUE_PUSH(&c->disconnect, &disconnect->queue);
}

void test_cluster_reconnect(struct test_cluster *c, raft_id id1, raft_id id2)
{
    queue *head;
    QUEUE_FOREACH(head, &c->disconnect)
    {
        struct disconnect *d = QUEUE_DATA(head, struct disconnect, queue);
        if (d->id1 == id1 && d->id2 == id2) {
            QUEUE_REMOVE(&d->queue);
            free(d);
            return;
        }
    }
}

void test_cluster_network_latency(struct test_cluster *c,
                                  raft_id id,
                                  unsigned msecs)
{
    struct test_server *server = clusterGetServer(c, id);
    server->network_latency = msecs;
}

void test_cluster_disk_latency(struct test_cluster *c,
                               raft_id id,
                               unsigned msecs)
{
    struct test_server *server = clusterGetServer(c, id);
    server->disk_latency = msecs;
}

void test_cluster_kill(struct test_cluster *c, raft_id id)
{
    struct test_server *server = clusterGetServer(c, id);
    serverKill(server);
}
