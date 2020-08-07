/* Setup and drive a test raft cluster. */

#ifndef TEST_CLUSTER_H
#define TEST_CLUSTER_H

#include <stdlib.h>

#include "../../include/raft.h"
#include "cluster.h"
#include "heap.h"
#include "munit.h"

/* Macro helpers. */
#define FIXTURE_CLUSTER \
    FIXTURE_HEAP;       \
    struct test_cluster cluster

#define SETUP_CLUSTER \
    SET_UP_HEAP;      \
    test_cluster_setup(params, &f->cluster)

#define TEAR_DOWN_CLUSTER                \
    test_cluster_tear_down(&f->cluster); \
    TEAR_DOWN_HEAP

#define CLUSTER_GROW(N) test_cluster_grow(&f->cluster, N)

#define CLUSTER_GET(ID) test_cluster_get(&f->cluster, ID)

#define CLUSTER_START(ID) test_cluster_start(&f->cluster, ID)

#define CLUSTER_STEP test_cluster_step(&f->cluster)

#define CLUSTER_FAIL(STATUS) test_cluster_fail(&f->cluster, STATUS)

#define CLUSTER_ELAPSE(MSECS) test_cluster_elapse(&f->cluster, MSECS)

#define CLUSTER_NETWORK_LATENCY(ID, MSECS) \
    test_cluster_network_latency(&f->cluster, ID, MSECS)

#define CLUSTER_DISK_LATENCY(ID, MSECS) \
    test_cluster_disk_latency(&f->cluster, ID, MSECS)

#define CLUSTER_TRACE(EXPECTED)                       \
    if (!test_cluster_trace(&f->cluster, EXPECTED)) { \
        munit_error("trace does not match");          \
    }

/* Append an entry to the log persisted on disk. */
#define CLUSTER_PERSIST_ENTRY(ID, ENTRY) \
    test_cluster_persist_entry(&f->cluster, ID, ENTRY)

#define CLUSTER_POPULATE_CONFIGURATION(CONF, N, N_VOTERS, N_STANDBYS)         \
    do {                                                                      \
        unsigned _i;                                                          \
        int __rv;                                                             \
        munit_assert_int(N, >=, 1);                                           \
        munit_assert_int(N_VOTERS, <=, N);                                    \
        raft_configuration_init(CONF);                                        \
        for (_i = 0; _i < N; _i++) {                                          \
            raft_id _id = _i + 1;                                             \
            int _role = RAFT_SPARE;                                           \
            char _address[64];                                                \
            if (_i < N_VOTERS) {                                              \
                _role = RAFT_VOTER;                                           \
            } else if (N_STANDBYS > 0 && (int)(_i - N_VOTERS) < N_STANDBYS) { \
                _role = RAFT_STANDBY;                                         \
            }                                                                 \
            sprintf(_address, "%llu", _id);                                   \
            __rv = raft_configuration_add(CONF, _id, _address, _role);        \
            munit_assert_int(__rv, ==, 0);                                    \
        }                                                                     \
    } while (0)

/* Convenience around CLUSTER_PERSIST_ENTRY, appending a RAFT_CHANGE entry to
 * the log persisted on disk. The configuration will have N servers with
 * consecutive IDs starting at 1, of which the first N_VOTERS ones will have the
 * RAFT_VOTER role, the subsequent N_SPARES the RAFT_STANDBY role and the rest
 * the RAFT_SPARE role.  */
#define CLUSTER_PERSIST_CONFIGURATION(ID, N, N_VOTING, N_SPARES)       \
    do {                                                               \
        struct raft_configuration _conf;                               \
        struct raft_entry _entry;                                      \
        int _rv;                                                       \
                                                                       \
        CLUSTER_POPULATE_CONFIGURATION(&_conf, N, N_VOTING, N_SPARES); \
        _entry.type = RAFT_CHANGE;                                     \
        _entry.term = 1;                                               \
        _rv = raft_configuration_encode(&_conf, &_entry.buf);          \
        munit_assert_int(_rv, ==, 0);                                  \
                                                                       \
        raft_configuration_close(&_conf);                              \
                                                                       \
        CLUSTER_PERSIST_ENTRY(ID, &_entry);                            \
        raft_free(_entry.buf.base);                                    \
    } while (0)

/* Convenience around CLUSTER_PERSIST_ENTRY, appending a RAFT_COMMAND entry to
 * the log persisted on disk. The payload will be a 64-bit intger with the given
 * value. */
#define CLUSTER_PERSIST_COMMAND(ID, TERM, N) \
    do {                                     \
        struct raft_entry _entry;            \
        int64_t _n = (int64_t)N;             \
        _entry.type = RAFT_COMMAND;          \
        _entry.term = TERM;                  \
        _entry.buf.len = sizeof _n;          \
        _entry.buf.base = &_n;               \
        CLUSTER_PERSIST_ENTRY(ID, &_entry);  \
    } while (0)

/* Set the persisted term of the given server to the given value. */
#define CLUSTER_PERSIST_TERM(ID, TERM) \
    test_cluster_persist_term(&f->cluster, ID, TERM)

#define CLUSTER_PERSIST_SNAPSHOT(ID, INDEX, TERM, CONF_N, CONF_N_VOTING, \
                                 CONF_INDEX)                             \
    do {                                                                 \
        struct raft_snapshot _snapshot;                                  \
        _snapshot.index = INDEX;                                         \
        _snapshot.term = TERM;                                           \
        CLUSTER_POPULATE_CONFIGURATION(&_snapshot.configuration, CONF_N, \
                                       CONF_N_VOTING, 0);                \
        _snapshot.configuration_index = CONF_INDEX;                      \
        _snapshot.data.len = 8;                                          \
        _snapshot.data.base = munit_malloc(_snapshot.data.len);          \
        test_cluster_persist_snapshot(&f->cluster, ID, &_snapshot);      \
        raft_configuration_close(&_snapshot.configuration);              \
        free(_snapshot.data.base);                                       \
    } while (0)

/* Convenience to persist the first configuration entry and set initial term. */
#define CLUSTER_BOOTSTRAP(ID, N, N_VOTING)                 \
    do {                                                   \
        CLUSTER_PERSIST_TERM(ID, 1);                       \
        CLUSTER_PERSIST_CONFIGURATION(ID, N, N_VOTING, 0); \
    } while (0)

/* Kill the server with the given ID. */
#define CLUSTER_KILL(ID) test_cluster_kill(&f->cluster, ID);

/* Assign the given role to the server that was added last. */
#define CLUSTER_ASSIGN(REQ, ROLE)                                              \
    do {                                                                       \
        unsigned _id;                                                          \
        int _rv;                                                               \
        _id = CLUSTER_N; /* Last server that was added. */                     \
        _rv = raft_assign(CLUSTER_RAFT(CLUSTER_LEADER), REQ, _id, ROLE, NULL); \
        munit_assert_int(_rv, ==, 0);                                          \
    } while (0)

/* Ensure that the cluster can make progress from the current state.
 *
 * - If no leader is present, wait for one to be elected.
 * - Submit a request to apply a new FSM command and wait for it to complete. */
#define CLUSTER_MAKE_PROGRESS                                          \
    {                                                                  \
        struct raft_apply *req_ = munit_malloc(sizeof *req_);          \
        if (!(CLUSTER_HAS_LEADER)) {                                   \
            CLUSTER_STEP_UNTIL_HAS_LEADER(10000);                      \
        }                                                              \
        CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, req_, 1, NULL);            \
        CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, req_->index, 3000); \
        free(req_);                                                    \
    }

/* Disconnect ID1 from ID2. */
#define CLUSTER_DISCONNECT(ID1, ID2) \
    test_cluster_disconnect(&f->cluster, ID1, ID2)

/* Reconnect ID1 to ID2. */
#define CLUSTER_RECONNECT(ID1, ID2) \
    test_cluster_reconnect(&f->cluster, ID1, ID2)

/* Persisted state of a single node.
 *
 * This data is passed to raft_start() when starting a server, and is updated as
 * the server makes progress. */
struct test_disk
{
    raft_term term;
    raft_id voted_for;
    struct raft_snapshot *snapshot;
    raft_index start_index;
    struct raft_entry *entries;
    unsigned n_entries;
};

/* Wrap a @raft instance and maintain disk and network state. */
struct test_cluster;
struct test_server
{
    struct test_disk disk;        /* Persisted data */
    struct raft_tracer tracer;    /* Custom tracer */
    struct raft_clock clock;      /* Exposes global time */
    struct raft raft;             /* Raft instance */
    struct test_cluster *cluster; /* Parent cluster */
    raft_time timeout;            /* Next scheduled tick */
    unsigned network_latency;     /* Network latency */
    unsigned disk_latency;        /* Disk latency */
    bool running;                 /* Whether the server is running */
};

/* Cluster of test raft servers instances with fake disk and network I/O. */
struct test_cluster
{
    raft_time time;               /* Global time, for all servers */
    struct test_server **servers; /* Array of pointers to server objects */
    unsigned n;                   /* Number of items in the servers array */
    char trace[8192];             /* Captured trace messages */
    void *operations[2];          /* Pending async operations */
    void *disconnect[2];          /* Disconnected servers */
    int status;                   /* Expected value of next step */
};

void test_cluster_setup(const MunitParameter params[], struct test_cluster *c);
void test_cluster_tear_down(struct test_cluster *c);

/* Return the global cluster time. */
raft_time test_cluster_now(struct test_cluster *c);

/* Compare the trace of all messages emitted by all servers with the given
 * expected trace. If they don't match, print the last line which differs and
 * return #false. */
bool test_cluster_trace(struct test_cluster *c, const char *expected);

/* Return the server with the given @id. */
struct raft *test_cluster_get(struct test_cluster *c, raft_id id);

/* Add @n new servers to the cluster. */
void test_cluster_grow(struct test_cluster *c, unsigned n);

/* Append an entry to the log persisted on disk of the server with the given
 * @id. Must be called before starting the server. */
void test_cluster_persist_entry(struct test_cluster *c,
                                raft_id id,
                                struct raft_entry *entry);

/* Set the persisted term of the given server to the given value. Must me called
 * before starting the server. */
void test_cluster_persist_term(struct test_cluster *c,
                               raft_id id,
                               raft_term term);

/* Set last persisted snapshot of the given server to the given value. Must me
 * called before starting the server. */
void test_cluster_persist_snapshot(struct test_cluster *c,
                                   raft_id id,
                                   struct raft_snapshot *snapshot);

/* Step through the cluster state advancing the time to the minimum value needed
 * for it to make progress (i.e. for a message to be delivered, for an I/O
 * operation to complete or for a single time tick to occur).
 *
 * In particular, the following happens:
 *
 * 1. The #RAFT_IO event queues across all servers are consumed. All events of
 *    type #RAFT_PERSIST_TERM_AND_VOTE are processed immediately, the relevant
 *    disk state updated, and the @raft_done() callback invoked. All other I/O
 *    events get pushed to the cluster #io queue. The #RAFT_PERSIST_ENTRIES
 *    requests also get assigned a completion time.
 *
 * 2. If there are pending #raft_send_message events in the cluster #io queue,
 *    the oldest one of them is picked and the relevant @raft_done() callback
 *    gets fired. This simulates the completion of a socket write, which means
 *    that the message has been sent. The receiver does not immediately receive
 *    the message, as the message is propagating through the network. However
 *    any memory associated with that #raft_send_message event can be released
 *    (e.g. log entries). The #cluster assigns a delivery time to the message,
 *    which mimics network latency. However, if the sender and the receiver are
 *    currently disconnected, the request is simply dropped. If a @raft_done()
 *    callback was fired, jump directly to 4. and skip 3.
 *
 * 3. If there are pending #raft_append disk write requests in the cluster #io
 *    queue, the one with the lowest completion time is picked. If there
 *    messages currently being transmitted, the one with the lowest delivery
 *    time is picked. All servers are scanned, and the one with the lowest tick
 *    expiration time is picked. The three times are compared and the lowest one
 *    is picked. If a #raft_append request has completed, the relevant
 *    @raft_done() callback will be invoked, if there's a network message to be
 *    delivered, the receiver's @raft_recv() callback gets fired, if a timeout
 *    has expired the relevant @raft_tick() callback will be invoked. Only one
 *    event will be fired. If there is more than one event to fire, one of them
 *    is picked according to the following rules: events for servers with lower
 *    ID are fired first, tick events take precedence over disk events, and disk
 *    events take precedence over network events.
 *
 * 4. The current cluster leader is detected (if any). When detecting the leader
 *    the Election Safety property is checked: no servers can be in leader state
 *    for the same term. The server in leader state with the highest term is
 *    considered the current cluster leader, as long as it's "stable", i.e. it
 *    has been acknowledged by all servers connected to it, and those servers
 *    form a majority (this means that no further leader change can happen,
 *    unless the network gets disrupted). If there is a stable leader and it has
 *    not changed with respect to the previous call to @test_cluster_step(),
 *    then the Leader Append-Only property is checked, by comparing its log with
 *    a copy of it that was taken during the previous iteration.
 *
 * 5. If there is a stable leader, its current log is copied, in order to be
 *    able to check the Leader Append-Only property at the next call.
 *
 * 6. If there is a stable leader, its commit index gets copied. */
void test_cluster_step(struct test_cluster *c);

/* Like test_cluster_step() but expect the relevant raft API call to fail with
 * the given value. */
void test_cluster_fail(struct test_cluster *c, int status);

/* Let the given number of milliseconds elapse. This requires that no event
 * would be triggered by test_cluster_step() in the given time window. */
void test_cluster_elapse(struct test_cluster *c, unsigned msecs);

/* Disconnect the server with @id1 from the one with @id2, so @id1 can't send
 * messages to @id2. Inflight messages will be discarded as well. */
void test_cluster_disconnect(struct test_cluster *c, raft_id id1, raft_id id2);

/* Reconnect two previously disconnected servers. */
void test_cluster_reconnect(struct test_cluster *c, raft_id id1, raft_id id2);

/* Set the network latency of outgoing messages on the given server. It will
 * apply to all messages sent from here on. */
void test_cluster_network_latency(struct test_cluster *c,
                                  raft_id id,
                                  unsigned msecs);

/* Set the disk latency of the given server. It will apply to all persist
 * requests submitted from here on. */
void test_cluster_disk_latency(struct test_cluster *c,
                               raft_id id,
                               unsigned msecs);

/* Start the server with the given @id, using the current state persisted on its
 * disk. */
void test_cluster_start(struct test_cluster *c, raft_id id);

/* Kill the server with the given ID which won't run anymore. */
void test_cluster_kill(struct test_cluster *c, raft_id id);

#endif /* TEST_CLUSTER_H */
