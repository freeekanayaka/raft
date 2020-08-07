#include "../../src/queue.h"

#include "cluster.h"
#include "munit.h"

/* Defaults */
#define NETWORK_LATENCY 15
#define DISK_LATENCY 10

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
    d->snapshot_metadata = NULL;
    d->start_index = 0;
    d->entries = NULL;
    d->n_entries = 0;
}

/* Release all memory used by the disk. */
static void diskClose(struct test_disk *d)
{
    unsigned i;

    for (i = 0; i < d->n_entries; i++) {
        free(d->entries[i].buf.base);
    }
    free(d->entries);
}

/* Bootstrap a server by setting the persisted term to #1 and the first entry to
 * a configuration of @n servers with consecutive IDs starting at 1, of which
 * the first @n_voting ones will be voting servers. */
static void diskBootstrap(struct test_disk *d, unsigned n, unsigned n_voting)
{
    struct raft_configuration conf;
    struct raft_buffer buf;
    unsigned i;
    int rv;

    munit_assert_int(n, >=, 1);
    munit_assert_int(n_voting, <=, n);

    munit_assert_int(d->term, ==, 0);
    munit_assert_ptr_null(d->entries);

    d->term = 1;
    d->start_index = 1;
    d->entries = munit_malloc(sizeof *d->entries);
    d->n_entries = 1;

    raft_configuration_init(&conf);
    for (i = 0; i < n; i++) {
        raft_id id = i + 1;
        int role = i < n_voting ? RAFT_VOTER : RAFT_STANDBY;
        char address[64];
        sprintf(address, "%llu", id);
        rv = raft_configuration_add(&conf, id, address, role);
        munit_assert_int(rv, ==, 0);
    }

    rv = raft_configuration_encode(&conf, &buf);
    munit_assert_int(rv, ==, 0);

    /* Copy the entry data, since we it has been allocated with
     * raft_malloc(). */
    d->entries[0].buf.base = munit_malloc(buf.len);
    d->entries[0].buf.len = buf.len;
    memcpy(d->entries[0].buf.base, buf.base, buf.len);
    raft_free(buf.base);

    d->entries[0].term = 1;
    d->entries[0].type = RAFT_CHANGE;

    raft_configuration_close(&conf);
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

/* Load all data persisted on the disk. */
static void diskLoad(struct test_disk *d,
                     raft_term *term,
                     raft_id *voted_for,
                     struct raft_snapshot_metadata **snapshot_metadata,
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
    *snapshot_metadata = d->snapshot_metadata;
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

/* Update term and vote. */
static void diskPersistTermAndVote(struct test_disk *d,
                                   raft_term term,
                                   raft_id voted_for)
{
    d->term = term;
    d->voted_for = voted_for;
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

/* Initialize a new server object. */
static void serverInit(struct test_server *s,
                       raft_id id,
                       struct test_cluster *cluster)
{
    int rv;

    diskInit(&s->disk);

    s->tracer.data = s;
    s->tracer.emit = serverEmit;

    rv = raft_init(&s->raft, id);
    munit_assert_int(rv, ==, 0);

    s->raft.tracer = &s->tracer;

    raft_seed(&s->raft, id); /* To generate different timeouts */

    s->cluster = cluster;
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
    struct raft_snapshot_metadata *snapshot_metadata;
    raft_index start_index;
    struct raft_entry *entries;
    unsigned n_entries;
    int rv;

    diskLoad(&s->disk, &term, &voted_for, &snapshot_metadata, &start_index,
             &entries, &n_entries);

    rv = raft_start(&s->raft, &s->cluster->clock, term, voted_for,
                    snapshot_metadata, start_index, entries, n_entries);
    munit_assert_int(rv, ==, 0);

    s->running = false;
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

static void serverHandlePersistTermAndVote(struct test_server *s,
                                           struct raft_io *io)
{
    struct raft_persist_term_and_vote *persist =
        (struct raft_persist_term_and_vote *)io;
    int rv;
    diskPersistTermAndVote(&s->disk, persist->term, persist->voted_for);
    rv = raft_done(&s->raft, io, 0);
    munit_assert_int(rv, ==, 0);
}

/* Pull all messages currently pending in this server's #io queue, and transfer
 * them to the cluster I/O queue. */
static void serverHandleIo(struct test_server *s)
{
    struct raft_io *io;
    while ((io = raft_pending(&s->raft))) {
        switch (io->type) {
            case RAFT_SEND_MESSAGE:
                serverHandleSendMessage(s, io);
                break;
            case RAFT_PERSIST_ENTRIES:
                break;
            case RAFT_PERSIST_TERM_AND_VOTE:
                serverHandlePersistTermAndVote(s, io);
                break;
        }
    }
}

/* Invoke raft_tick(). */
static void serverTick(struct test_server *s)
{
    int rv;
    s->cluster->time = s->raft.timeout;
    rv = raft_tick(&s->raft);
    munit_assert_int(rv, ==, 0);
}

static void serverCompleteSendMessage(struct test_server *s,
                                      struct raft_send_message *send)
{
    struct operation *operation = munit_malloc(sizeof *operation);

    operation->type = OPERATION_TRANSMIT;
    operation->id = send->id;

    operation->completion = s->cluster->time + NETWORK_LATENCY;
    operation->transmit.from = s->raft.id;
    operation->transmit.message = send->message;

    QUEUE_PUSH(&s->cluster->operations, &operation->queue);
}

static void serverCompleteIo(struct test_server *s, struct operation *operation)
{
    struct raft_io *io = operation->io;
    int rv;
    switch (io->type) {
        case RAFT_SEND_MESSAGE:
            serverCompleteSendMessage(s, (struct raft_send_message *)io);
            break;
        case RAFT_PERSIST_ENTRIES:
            break;
        case RAFT_PERSIST_TERM_AND_VOTE:
            munit_error("unexpected persist term and vote request");
            break;
    };
    /* TODO: simulate I/O errors and pass status != 0 */
    rv = raft_done(&s->raft, io, 0);
    munit_assert_int(rv, ==, 0);
}

static void serverCompleteTransmit(struct test_server *s,
                                   struct operation *operation)
{
    char address[64];
    queue *head;
    int rv;

    /* Check if there's a disconnection. */
    QUEUE_FOREACH(head, &s->cluster->disconnect)
    {
        struct disconnect *d = QUEUE_DATA(head, struct disconnect, queue);
        if (d->id1 == operation->transmit.from && d->id2 == s->raft.id) {
            return;
        }
    }

    sprintf(address, "%llu", operation->transmit.from);
    rv = raft_recv(&s->raft, operation->transmit.from,
                   address, &operation->transmit.message);
    munit_assert_int(rv, ==, 0);
}

/* Complete either a @raft_io or transmit operation. */
static void serverCompleteOperation(struct test_server *s,
                                    struct operation *operation)
{
    s->cluster->time = operation->completion;
    QUEUE_REMOVE(&operation->queue);
    switch (operation->type) {
        case OPERATION_IO:
            serverCompleteIo(s, operation);
            break;
        case OPERATION_TRANSMIT:
            serverCompleteTransmit(s, operation);
            break;
    }
    free(operation);
}

/* Return the server with the given @id. */
static struct test_server *clusterGetServer(struct test_cluster *c, raft_id id)
{
    munit_assert_int(id, <=, c->n);
    return c->servers[id - 1];
}

static raft_time clusterClockNow(struct raft_clock *c)
{
    return *(raft_time *)c->data;
}

void test_cluster_setup(const MunitParameter params[], struct test_cluster *c)
{
    (void)params;
    c->time = 0;
    c->clock.now = clusterClockNow;
    c->clock.data = &c->time;
    c->servers = NULL;
    c->n = 0;
    QUEUE_INIT(&c->operations);
    QUEUE_INIT(&c->disconnect);
}

void test_cluster_tear_down(struct test_cluster *c)
{
    unsigned i;

    /* Drop pending operations */
    while (!QUEUE_IS_EMPTY(&c->operations)) {
        struct operation *operation;
        queue *head;
        head = QUEUE_HEAD(&c->operations);
        operation = QUEUE_DATA(head, struct operation, queue);
        switch (operation->type) {
            case OPERATION_IO:
                raft_free(operation->io);
                break;
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
    size_t n1 = strlen(c->trace);
    size_t n2 = strlen(expected);
    size_t match = 0; /* End of last matching line. */
    size_t i;

    for (i = 0; i < n1 && i < n2; i++) {
        if (c->trace[i] == expected[i]) {
            if (c->trace[i] == '\n') {
                match = i;
            }
            continue;
        }
        break;
    }

    if (i != n1 || i != n2) {
        char line[1024];
        size_t j;

        fprintf(stderr, "==> Expected:\n");
        line[0] = 0;
        for (j = 0; j + match + 1 < n2; j++) {
            char l = expected[j + match + 1];
            if (l == '\n') {
                line[j] = 0;
                break;
            }
            line[j] = l;
        }
        fprintf(stderr, "%s\n", line);

        fprintf(stderr, "==> Actual:\n");
        line[0] = 0;
        for (j = 0; j + match + 1 < n1; j++) {
            char l = c->trace[j + match + 1];
            if (l == '\n') {
                line[j] = 0;
                break;
            }
            line[j] = l;
        }
        fprintf(stderr, "%s\n", line);

        return false;
    }

    c->trace[0] = 0;

    return true;
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

void test_cluster_bootstrap(struct test_cluster *c,
                            raft_id id,
                            unsigned n,
                            unsigned n_voting)
{
    struct test_server *server = clusterGetServer(c, id);
    diskBootstrap(&server->disk, n, n_voting);
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
        if (server == NULL || other->raft.timeout < server->raft.timeout) {
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

    clusterHandleIo(c);
    server = clusterGetServerWithEarliestTimeout(c);
    operation = clusterGetOperationWithEarliestCompletion(c);

    if (operation == NULL || server->raft.timeout < operation->completion) {
        serverTick(server);
    } else {
        server = clusterGetServer(c, operation->id);
        serverCompleteOperation(server, operation);
    }
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
