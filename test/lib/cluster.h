/* Setup and drive a test raft cluster. */

#ifndef TEST_CLUSTER_H
#define TEST_CLUSTER_H

#include <stdlib.h>

#include "../../include/raft.h"
#include "../../include/raft/fixture.h"
#include "cluster.h"
#include "fsm.h"
#include "heap.h"
#include "munit.h"
#include "snapshot.h"

#define CLUSTER_N_MAX 8

/* Macro helpers. */
#define FIXTURE_CLUSTER \
    FIXTURE_HEAP;       \
    struct test_cluster cluster

#define SETUP_CLUSTER \
    SET_UP_HEAP;       \
    test_cluster_setup(params, &f->cluster)

#define TEAR_DOWN_CLUSTER                \
    test_cluster_tear_down(&f->cluster); \
    TEAR_DOWN_HEAP

#define CLUSTER_NOW test_cluster_now(&f->cluster)

#define CLUSTER_GROW(N) test_cluster_grow(&f->cluster, N)

#define CLUSTER_GET(ID) test_cluster_get(&f->cluster, ID)

#define CLUSTER_BOOTSTRAP(ID, N, N_VOTING) \
    test_cluster_bootstrap(&f->cluster, ID, N, N_VOTING)

#define CLUSTER_START(ID) test_cluster_start(&f->cluster, ID)

#define CLUSTER_STEP test_cluster_step(&f->cluster)

#define CLUSTER_TRACE(EXPECTED)                       \
    if (!test_cluster_trace(&f->cluster, EXPECTED)) { \
        munit_error("trace does not match");          \
    }

/* Munit parameter for setting the number of servers */
#define CLUSTER_N_PARAM "cluster-n"

/* Munit parameter for setting the number of voting servers */
#define CLUSTER_N_VOTING_PARAM "cluster-n-voting"

/* Munit parameter for enabling pre-vote */
#define CLUSTER_PRE_VOTE_PARAM "cluster-pre-vote"

/* Get the number of servers in the cluster. */
#define CLUSTER_N raft_fixture_n(&f->cluster)

/* Index of the current leader, or CLUSTER_N if there's no leader. */
#define CLUSTER_LEADER raft_fixture_leader_index(&f->cluster)

/* True if the cluster has a leader. */
#define CLUSTER_HAS_LEADER CLUSTER_LEADER < CLUSTER_N

/* Get the struct raft object of the I'th server. */
#define CLUSTER_RAFT(I) raft_fixture_get(&f->cluster, I)

/* Assert the state of the server with the given ID. */
#define CLUSTER_STATE(ID, STATE) \
    munit_assert_int(raft_state(CLUSTER_GET(ID)), ==, STATE)

/* Append an entry to the log persisted on disk. */
#define CLUSTER_PERSIST_ENTRY(ID, ENTRY) \
    test_cluster_persist_entry(&f->cluster, ID, ENTRY)

/* Append a RAFT_COMMAND entry to the log persisted on disk. The payload will be
 * a 64-bit intger with the given value. */
#define CLUSTER_PERSIST_COMMAND(ID, TERM, N) \
    do {                                     \
        struct raft_entry entry_;            \
        int64_t n_ = (int64_t)N;             \
        entry_.type = RAFT_COMMAND;          \
        entry_.term = TERM;                  \
        entry_.buf.len = sizeof n_;          \
        entry_.buf.base = &n_;               \
        CLUSTER_PERSIST_ENTRY(ID, &entry_);  \
    } while (0)

/* Set the persisted term of the given server to the given value. */
#define CLUSTER_PERSIST_TERM(ID, TERM) \
    test_cluster_persist_term(&f->cluster, ID, TERM)

/* Get the struct fsm object of the I'th server. */
#define CLUSTER_FSM(I) &f->fsms[I]

/* Return the last applied index on the I'th server. */
#define CLUSTER_LAST_APPLIED(I) \
    raft_last_applied(raft_fixture_get(&f->cluster, I))

/* Return the ID of the server the I'th server has voted for. */
#define CLUSTER_VOTED_FOR(I) raft_fixture_voted_for(&f->cluster, I)

/* Return a description of the last error occured on the I'th server. */
#define CLUSTER_ERRMSG(I) raft_errmsg(CLUSTER_RAFT(I))

/* Populate the given configuration with all servers in the fixture. All servers
 * will be voting. */
#define CLUSTER_CONFIGURATION(CONF)                                     \
    {                                                                   \
        int rv_;                                                        \
        rv_ = raft_fixture_configuration(&f->cluster, CLUSTER_N, CONF); \
        munit_assert_int(rv_, ==, 0);                                   \
    }

/* Bootstrap all servers in the cluster. All servers will be voting, unless the
 * cluster-n-voting parameter is used. */
/* #define CLUSTER_BOOTSTRAP \ */
/*     { \ */
/*         unsigned n_ = CLUSTER_N; \ */
/*         int rv_; \ */
/*         struct raft_configuration configuration; \ */
/*         if (munit_parameters_get(params, CLUSTER_N_VOTING_PARAM) != NULL) {
 * \ */
/*             n_ = atoi(munit_parameters_get(params, CLUSTER_N_VOTING_PARAM));
 * \ */
/*         } \ */
/*         rv_ = raft_fixture_configuration(&f->cluster, n_, &configuration);
 * \ */
/*         munit_assert_int(rv_, ==, 0); \ */
/*         rv_ = raft_fixture_bootstrap(&f->cluster, &configuration); \ */
/*         munit_assert_int(rv_, ==, 0); \ */
/*         raft_configuration_close(&configuration); \ */
/*     } */

/* Bootstrap all servers in the cluster. Only the first N servers will be
 * voting. */
#define CLUSTER_BOOTSTRAP_N_VOTING(N)                                      \
    {                                                                      \
        int rv_;                                                           \
        struct raft_configuration configuration_;                          \
        rv_ = raft_fixture_configuration(&f->cluster, N, &configuration_); \
        munit_assert_int(rv_, ==, 0);                                      \
        rv_ = raft_fixture_bootstrap(&f->cluster, &configuration_);        \
        munit_assert_int(rv_, ==, 0);                                      \
        raft_configuration_close(&configuration_);                         \
    }

/* Start the cluster as a new with a configuration containing the first N
 * servers as voters. */
#define CLUSTER_START_AS_NEW(N_VOTERS)                                         \
    do {                                                                       \
        struct raft_configuration _conf;                                       \
        unsigned _i;                                                           \
        int _rv;                                                               \
        raft_configuration_init(&_conf);                                       \
        for (_i = 0; _i < N_VOTERS; _i++) {                                    \
            raft_id _id = _i + 1;                                              \
            char _address[64];                                                 \
            int _role = _i < N_VOTERS ? RAFT_VOTER : RAFT_STANDBY;             \
            sprintf(_address, "%llu", _id);                                    \
            _rv = raft_configuration_add(&_conf, _id, _address, _role);        \
            munit_assert_int(_rv, ==, 0);                                      \
        }                                                                      \
        for (_i = 0; _i < N_VOTERS; _i++) {                                    \
            struct raft_entry *_entry = raft_malloc(sizeof *_entry);           \
            munit_assert_ptr_not_null(_entry);                                 \
            _entry->term = 1;                                                  \
            _entry->type = RAFT_CHANGE;                                        \
            _entry->batch = NULL;                                              \
            raft_configuration_encode(&_conf, &_entry->buf);                   \
            _rv = raft_fixture_add(&f->cluster, _i + 1, 1, 0, NULL, 1, _entry, \
                                   1);                                         \
            munit_assert_int(_rv, ==, 0);                                      \
        }                                                                      \
        raft_configuration_close(&_conf);                                      \
    } while (0)

/* Start all servers in the test cluster. */
/* #define CLUSTER_START                         \ */
/*     {                                         \ */
/*         int rc;                               \ */
/*         rc = raft_fixture_start(&f->cluster); \ */
/*         munit_assert_int(rc, ==, 0);          \ */
/*     } */

/* Step the cluster. */
/* #define CLUSTER_STEP raft_fixture_step(&f->cluster); */

/* Step the cluster N times. */
#define CLUSTER_STEP_N(N)                   \
    {                                       \
        unsigned i_;                        \
        for (i_ = 0; i_ < N; i_++) {        \
            raft_fixture_step(&f->cluster); \
        }                                   \
    }

/* Step until the given function becomes true. */
#define CLUSTER_STEP_UNTIL(FUNC, ARG, MSECS)                            \
    {                                                                   \
        bool done_;                                                     \
        done_ = raft_fixture_step_until(&f->cluster, FUNC, ARG, MSECS); \
        munit_assert_true(done_);                                       \
    }

/* Step the cluster until a leader is elected or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_ELAPSED(MSECS) \
    raft_fixture_step_until_elapsed(&f->cluster, MSECS)

/* Step the cluster until a leader is elected or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_HAS_LEADER(MAX_MSECS)                           \
    {                                                                      \
        bool done;                                                         \
        done = raft_fixture_step_until_has_leader(&f->cluster, MAX_MSECS); \
        munit_assert_true(done);                                           \
        munit_assert_true(CLUSTER_HAS_LEADER);                             \
    }

/* Step the cluster until there's no leader or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_HAS_NO_LEADER(MAX_MSECS)                           \
    {                                                                         \
        bool done;                                                            \
        done = raft_fixture_step_until_has_no_leader(&f->cluster, MAX_MSECS); \
        munit_assert_true(done);                                              \
        munit_assert_false(CLUSTER_HAS_LEADER);                               \
    }

/* Step the cluster until the given index was applied by the given server (or
 * all if N) or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_APPLIED(I, INDEX, MAX_MSECS)                        \
    {                                                                          \
        bool done;                                                             \
        done =                                                                 \
            raft_fixture_step_until_applied(&f->cluster, I, INDEX, MAX_MSECS); \
        munit_assert_true(done);                                               \
    }

/* Step the cluster until the state of the server with the given index matches
 * the given value, or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_STATE_IS(I, STATE, MAX_MSECS)               \
    {                                                                  \
        bool done;                                                     \
        done = raft_fixture_step_until_state_is(&f->cluster, I, STATE, \
                                                MAX_MSECS);            \
        munit_assert_true(done);                                       \
    }

/* Step the cluster until the term of the server with the given index matches
 * the given value, or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_TERM_IS(I, TERM, MAX_MSECS)                        \
    {                                                                         \
        bool done;                                                            \
        done =                                                                \
            raft_fixture_step_until_term_is(&f->cluster, I, TERM, MAX_MSECS); \
        munit_assert_true(done);                                              \
    }

/* Step the cluster until server I has voted for server J, or #MAX_MSECS have
 * elapsed. */
#define CLUSTER_STEP_UNTIL_VOTED_FOR(I, J, MAX_MSECS)                        \
    {                                                                        \
        bool done;                                                           \
        done =                                                               \
            raft_fixture_step_until_voted_for(&f->cluster, I, J, MAX_MSECS); \
        munit_assert_true(done);                                             \
    }

/* Step the cluster until all messages from server I to server J have been
 * delivered, or #MAX_MSECS elapse. */
#define CLUSTER_STEP_UNTIL_DELIVERED(I, J, MAX_MSECS)                        \
    {                                                                        \
        bool done;                                                           \
        done =                                                               \
            raft_fixture_step_until_delivered(&f->cluster, I, J, MAX_MSECS); \
        munit_assert_true(done);                                             \
    }

/* Request to apply an FSM command to add the given value to x. */
#define CLUSTER_APPLY_ADD_X(I, REQ, VALUE, CB)      \
    {                                               \
        struct raft_buffer buf_;                    \
        struct raft *raft_;                         \
        int rv_;                                    \
        FsmEncodeAddX(VALUE, &buf_);                \
        raft_ = raft_fixture_get(&f->cluster, I);   \
        rv_ = raft_apply(raft_, REQ, &buf_, 1, CB); \
        munit_assert_int(rv_, ==, 0);               \
    }

/* Kill the I'th server. */
#define CLUSTER_KILL(I) raft_fixture_kill(&f->cluster, I);

/* Kill the leader. */
#define CLUSTER_KILL_LEADER CLUSTER_KILL(CLUSTER_LEADER)

/* Kill a majority of servers, except the leader (if there is one). */
#define CLUSTER_KILL_MAJORITY                                \
    {                                                        \
        size_t i2;                                           \
        size_t n;                                            \
        for (i2 = 0, n = 0; n < (CLUSTER_N / 2) + 1; i2++) { \
            if (i2 == CLUSTER_LEADER) {                      \
                continue;                                    \
            }                                                \
            CLUSTER_KILL(i2)                                 \
            n++;                                             \
        }                                                    \
    }

/* Add a new pristine server to the cluster, connected to all others. Then
 * submit a request to add it to the configuration as an idle server. */
/* #define CLUSTER_ADD(REQ)                                               \ */
/*     {                                                                  \ */
/*         int rc;                                                        \ */
/*         struct raft *new_raft;                                         \ */
/*         CLUSTER_GROW;                                                  \ */
/*         rc = raft_start(CLUSTER_RAFT(CLUSTER_N - 1));                  \ */
/*         munit_assert_int(rc, ==, 0);                                   \ */
/*         new_raft = CLUSTER_RAFT(CLUSTER_N - 1);                        \ */
/*         rc = raft_add(CLUSTER_RAFT(CLUSTER_LEADER), REQ, new_raft->id, \ */
/*                       new_raft->address, NULL);                        \ */
/*         munit_assert_int(rc, ==, 0);                                   \ */
/*     } */

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

/* Elect the I'th server. */
#define CLUSTER_ELECT(I) raft_fixture_elect(&f->cluster, I)

/* Depose the current leader */
#define CLUSTER_DEPOSE raft_fixture_depose(&f->cluster)

/* Disconnect ID1 from ID2. */
#define CLUSTER_DISCONNECT(ID1, ID2) \
    test_cluster_disconnect(&f->cluster, ID1, ID2)

/* Reconnect ID1 to ID2. */
#define CLUSTER_RECONNECT(ID1, ID2) \
    test_cluster_reconnect(&f->cluster, ID1, ID2)

/* Saturate the connection from I to J. */
#define CLUSTER_SATURATE(I, J) raft_fixture_saturate(&f->cluster, I, J)

/* Saturate the connection from I to J and from J to I, in both directions. */
#define CLUSTER_SATURATE_BOTHWAYS(I, J) \
    CLUSTER_SATURATE(I, J);             \
    CLUSTER_SATURATE(J, I)

/* Desaturate the connection between I and J, making messages flow again. */
#define CLUSTER_DESATURATE(I, J) raft_fixture_desaturate(&f->cluster, I, J)

/* Reconnect two servers. */
#define CLUSTER_DESATURATE_BOTHWAYS(I, J) \
    CLUSTER_DESATURATE(I, J);             \
    CLUSTER_DESATURATE(J, I)

/* Set the network latency of outgoing messages of server I. */
#define CLUSTER_SET_NETWORK_LATENCY(I, MSECS) \
    raft_fixture_set_network_latency(&f->cluster, I, MSECS)

/* Set the disk I/O latency of server I. */
#define CLUSTER_SET_DISK_LATENCY(I, MSECS) \
    raft_fixture_set_disk_latency(&f->cluster, I, MSECS)

/* Set the term persisted on the I'th server. This must be called before
 * starting the cluster. */
#define CLUSTER_SET_TERM(I, TERM) raft_fixture_set_term(&f->cluster, I, TERM)

/* Set the snapshot persisted on the I'th server. This must be called before
 * starting the cluster. */
#define CLUSTER_SET_SNAPSHOT(I, LAST_INDEX, LAST_TERM, CONF_INDEX, X, Y)  \
    {                                                                     \
        struct raft_configuration configuration_;                         \
        struct raft_snapshot *snapshot_;                                  \
        CLUSTER_CONFIGURATION(&configuration_);                           \
        CREATE_SNAPSHOT(snapshot_, LAST_INDEX, LAST_TERM, configuration_, \
                        CONF_INDEX, X, Y);                                \
        raft_fixture_set_snapshot(&f->cluster, I, snapshot_);             \
    }

/* Add a persisted entry to the I'th server. This must be called before
 * starting the cluster. */
#define CLUSTER_ADD_ENTRY(I, ENTRY) \
    raft_fixture_add_entry(&f->cluster, I, ENTRY)

/* Add an entry to the ones persisted on the I'th server. This must be called
 * before starting the cluster. */
#define CLUSTER_ADD_ENTRY(I, ENTRY) \
    raft_fixture_add_entry(&f->cluster, I, ENTRY)

/* Make an I/O error occur on the I'th server after @DELAY operations. */
#define CLUSTER_IO_FAULT(I, DELAY, REPEAT) \
    raft_fixture_io_fault(&f->cluster, I, DELAY, REPEAT)

/* Return the number of messages sent by the given server. */
#define CLUSTER_N_SEND(I, TYPE) raft_fixture_n_send(&f->cluster, I, TYPE)

/* Return the number of messages sent by the given server. */
#define CLUSTER_N_RECV(I, TYPE) raft_fixture_n_recv(&f->cluster, I, TYPE)

/* Set a fixture hook that randomizes election timeouts, disk latency and
 * network latency. */
#define CLUSTER_RANDOMIZE                \
    cluster_randomize_init(&f->cluster); \
    raft_fixture_hook(&f->cluster, cluster_randomize)

void cluster_randomize_init(struct raft_fixture *f);
void cluster_randomize(struct raft_fixture *f,
                       struct raft_fixture_event *event);

/* Persisted state of a single node.
 *
 * This data is passed to raft_start() when starting a server, and is updated as
 * the server makes progress. */
struct test_disk
{
    raft_term term;
    raft_id voted_for;
    struct raft_snapshot_metadata *snapshot_metadata;
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
    struct raft raft;             /* Raft instance */
    struct test_cluster *cluster; /* Parent cluster */
    bool running;                 /* Whether the server is running */
};

/* Cluster of test raft servers instances with fake disk and network I/O. */
struct test_cluster
{
    raft_time time;               /* Global time, for all servers */
    struct raft_clock clock;      /* Exposes global time */
    struct test_server **servers; /* Array of pointers to server objects */
    unsigned n;                   /* Number of items in the servers array */
    char trace[8192];             /* Captured trace messages */
    void *operations[2];          /* Pending async operations */
    void *disconnect[2];          /* Disconnected servers */
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

/* Bootstrap the server with the given @id by setting the persisted term to #1
 * and the first entry to a configuration of @n servers with consecutive IDs
 * starting at 1, of which the first @n_voting ones will be voting servers. */
void test_cluster_bootstrap(struct test_cluster *c,
                            raft_id id,
                            unsigned n,
                            unsigned n_voting);

/* Append an entry to the log persisted on disk of the server with the given
 * @id. Must be called before starting the server. */
void test_cluster_persist_entry(struct test_cluster *c,
                                raft_id id,
                                struct raft_entry *entry);

/* Set the persisted term of the given server to the given value. */
void test_cluster_persist_term(struct test_cluster *c,
                               raft_id id,
                               raft_term term);

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

/* Disconnect the server with @id1 from the one with @id2, so @id1 can't send
 * messages to @id2. Inflight messages will be discarded as well. */
void test_cluster_disconnect(struct test_cluster *c, raft_id id1, raft_id id2);

/* Reconnect two previously disconnected servers. */
void test_cluster_reconnect(struct test_cluster *c, raft_id id1, raft_id id2);

/* Start the server with the given @id, using the current state persisted on its
 * disk. */
void test_cluster_start(struct test_cluster *c, raft_id id);

#endif /* TEST_CLUSTER_H */
