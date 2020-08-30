#include "../lib/cluster.h"
#include "../lib/runner.h"

struct fixture
{
    FIXTURE_CLUSTER;
};

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/* Assert the values of the committed and uncommitted configuration indexes on
 * the raft instance with the given index. */
#define ASSERT_CONFIGURATION_INDEXES(ID, COMMITTED, UNCOMMITTED)     \
    {                                                                \
        struct raft *raft_ = CLUSTER_GET(ID);                        \
        munit_assert_int(raft_->configuration_index, ==, COMMITTED); \
        munit_assert_int(raft_->configuration_uncommitted_index, ==, \
                         UNCOMMITTED);                               \
    }

/* Assert that the state of the current catch up round matches the given
 * values. */
#define ASSERT_CATCH_UP_ROUND(I, PROMOTEED_ID, NUMBER, DURATION)               \
    {                                                                          \
        struct raft *raft_ = CLUSTER_GET(I);                                   \
        munit_assert_int(raft_->leader_state.promotee_id, ==, PROMOTEED_ID);   \
        munit_assert_int(raft_->leader_state.round_number, ==, NUMBER);        \
        munit_assert_int(                                                      \
            raft_->clock->now(raft_->clock) - raft_->leader_state.round_start, \
            >=, DURATION);                                                     \
    }

SUITE(raft_assign)

/* Assigning the voter role to a stand-by server whose log is already up-to-date
 * results in the relevant configuration change to be submitted immediately. */
TEST(raft_assign, promoteUpToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration conf;
    struct raft_entry entry;
    struct raft *raft;
    struct raft_server *server;
    int rv;
    CLUSTER_GROW(3);

    CLUSTER_PERSIST_TERM(1, 1 /* term */);
    CLUSTER_PERSIST_TERM(2, 1 /* term */);
    CLUSTER_PERSIST_TERM(3, 1 /* term */);
    CLUSTER_PERSIST_CONFIGURATION(1, 3, 2 /* votes */, 1 /* stand-bys */);
    CLUSTER_PERSIST_CONFIGURATION(2, 3, 2 /* votes */, 1 /* stand-bys */);
    CLUSTER_PERSIST_CONFIGURATION(3, 3, 2 /* votes */, 1 /* stand-bys */);

    /* Server 1 becomes leader. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "           probe server 3 sending 1 entries with first index 2\n");

    /* Servers 2 and 3 replicate the no-op entry and report success. */
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 140] 3 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n"
        "[ 150] 1 > recv append entries result from server 3\n"
        "           replicated index 2 lower or equal than commit index 2\n");

    CLUSTER_POPULATE_CONFIGURATION(&conf, 3, 3, 0);
    entry.type = RAFT_CHANGE;
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&conf);

    raft = CLUSTER_GET(1);
    rv = raft_accept(raft, &entry, 1);
    munit_assert_int(rv, ==, 0);

    /* Server 1 has already submitted the configuration change entry. */
    ASSERT_CONFIGURATION_INDEXES(1, 1, 3);

    /* Server 3 is being considered as voting, even though the configuration
     * change is not committed yet. */
    server = &raft->configuration.servers[2];
    munit_assert_int(server->role, ==, RAFT_VOTER);

    /* The configuration change eventually succeeds. */
    CLUSTER_TRACE(
        "[ 150] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "           pipeline server 2 sending 1 entries with first index 3\n"
        "           pipeline server 3 sending 1 entries with first index 3\n"
        "[ 160] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 160] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 160] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 160] 1 > tick\n"
        "[ 170] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 170] 3 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 180] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 3\n");

    return MUNIT_OK;
}

/* Assigning the voter role to a spare server whose log is not up-to-date
 * results in catch-up rounds to start. When the server has caught up, the
 * configuration change request gets submitted. */
TEST(raft_assign, promoteCatchUp, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_configuration conf;
    struct raft_server *server;
    struct raft *raft;
    int rv;

    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 0, vote 0, no snapshot, no entries\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n");

    CLUSTER_POPULATE_CONFIGURATION(&conf, 3, 3, 0);

    entry.type = RAFT_CHANGE;
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&conf);

    raft = CLUSTER_GET(1);
    rv = raft_accept(raft, &entry, 1);
    munit_assert_int(rv, ==, 0);

    /* Server 3 is not being considered as voting, since its log is behind. */
    raft = CLUSTER_GET(1);
    server = &raft->configuration.servers[2];
    munit_assert_int(server->role, ==, RAFT_SPARE);

    /* Advance the match index of server 3, by acknowledging the AppendEntries
     * request that the leader has sent to it. */
    CLUSTER_TRACE(
        "[ 120] 1 > accept 1 commands starting at 3\n"
        "           probe server 3 sending 1 entries with first index 2\n"
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           remote term is higher (2 vs 0) -> bump term\n"
        "           no entry at index 1 -> reject\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "           log mismatch -> send old entries to 3\n"
        "           probe server 3 sending 2 entries with first index 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n"
        "[ 150] 3 > recv append entries from server 1\n"
        "           persist 2 entries with first index 1\n"
        "[ 160] 3 > done persist 2 entries with first index 1 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 160] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 3\n");

    /* Disconnect server 2, so it doesn't participate in the quorum */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);

    /* Eventually the leader notices that the third server has caught. */
    CLUSTER_TRACE(
        "[ 170] 1 > recv append entries result from server 3\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 2 sending 1 entries with first index 3\n"
        "           pipeline server 3 sending 1 entries with first index 3\n"
        "           replicated index 2 lower or equal than commit index 2\n");

    munit_assert_int(raft->state, ==, RAFT_LEADER);
    munit_assert_int(raft->leader_state.promotee_id, ==, 0);

    /* The leader has submitted a configuration change request, but it's
     * uncommitted. */
    ASSERT_CONFIGURATION_INDEXES(1, 1, 3);

    /* The third server notifies that it has appended the new
     * configuration. Since it's considered voting already, it counts for the
     * majority and the entry gets committed. */
    CLUSTER_TRACE(
        "[ 180] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 180] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 190] 3 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 200] 1 > recv append entries result from server 3\n"
        "           replication quorum reached -> commit index 3\n");

    /* The promotion is completed. */
    ASSERT_CONFIGURATION_INDEXES(1, 3, 0);

    return MUNIT_OK;
}

/* Assigning the voter role to a spare server whose log is not up-to-date
 * results in catch-up rounds to start. If new entries are appended after a
 * round is started, a new round is initiated once the former one completes. */
TEST(raft_assign, promoteNewRound, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_configuration conf;
    struct raft *raft;
    int rv;

    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 0, vote 0, no snapshot, no entries\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n");

    /* Submit a configuration change to promote server 3 to voter. */
    CLUSTER_POPULATE_CONFIGURATION(&conf, 3, 3, 0);

    entry.type = RAFT_CHANGE;
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&conf);

    raft = CLUSTER_GET(1);
    rv = raft_accept(raft, &entry, 1);
    munit_assert_int(rv, ==, 0);

    CLUSTER_TRACE(
        "[ 120] 1 > accept 1 commands starting at 3\n"
        "           probe server 3 sending 1 entries with first index 2\n");

    /* The catch-up has round started and server 3 receives all entries that
     * server 1 had at the beginning of the round. */
    ASSERT_CATCH_UP_ROUND(1, 3 /* promotee */, 1 /* n */, 0 /* duration */);
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           remote term is higher (2 vs 0) -> bump term\n"
        "           no entry at index 1 -> reject\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "           log mismatch -> send old entries to 3\n"
        "           probe server 3 sending 2 entries with first index 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n"
        "[ 150] 3 > recv append entries from server 1\n"
        "           persist 2 entries with first index 1\n");

    /* Now submit a new entry to server 1 and also set a high network latency on
     * server 3 , so it won't deliver the AppendEntry results within an election
     * timeout, therefore the leader won't trigger the actual promotion just yet
     * and will start a new round instead. */
    entry.type = RAFT_COMMAND;
    entry.buf.base = raft_malloc(8);
    entry.buf.len = 8;
    rv = raft_accept(CLUSTER_GET(1), &entry, 1);
    munit_assert_int(rv, ==, 0);

    CLUSTER_NETWORK_LATENCY(3, 70);

    /* The duration of the first catch-up round eventually reaches the value of
     * the election timeout. */
    CLUSTER_TRACE(
        "[ 150] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "           pipeline server 2 sending 1 entries with first index 3\n"
        "[ 160] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 160] 3 > done persist 2 entries with first index 1 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 160] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 160] 1 > tick\n"
        "[ 170] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 180] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 3\n"
        "[ 200] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 4\n"
        "           probe server 3 sending 3 entries with first index 1\n"
        "[ 210] 2 > recv append entries from server 1\n"
        "[ 210] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 220] 3 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 220] 1 > recv append entries result from server 2\n");
    ASSERT_CATCH_UP_ROUND(1, 3, 1, 100);

    /* Server 1 now receives the AppendEntries result from serve 3 acknowledging
     * all entries except the last one. However, the catch up round duration is
     * now past an election timeout, so a new round gets started. */
    CLUSTER_TRACE(
        "[ 230] 1 > recv append entries result from server 3\n"
        "           replicated index 2 lower or equal than commit index 3\n"
        "           pipeline server 3 sending 1 entries with first index 3\n");

    ASSERT_CATCH_UP_ROUND(1, 3, 2, 0);

    /* Set the network latency of server 3 back to normal, so eventually server
     * 3 gets promoted. */
    CLUSTER_NETWORK_LATENCY(3, 10);

    CLUSTER_TRACE(
        "[ 240] 3 > recv append entries from server 1\n"
        "[ 240] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 4\n"
        "[ 250] 1 > recv append entries result from server 3\n"
        "           persist 1 entries with first index 4\n"
        "           pipeline server 2 sending 1 entries with first index 4\n"
        "           pipeline server 3 sending 1 entries with first index 4\n"
        "           replicated index 3 lower or equal than commit index 3\n"
        "[ 250] 2 > recv append entries from server 1\n"
        "[ 260] 1 > done persist 1 entries with first index 4 (status 0)\n"
        "           replication quorum not reached for index 4\n"
        "[ 260] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 4\n"
        "[ 260] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 4\n"
        "[ 260] 1 > recv append entries result from server 2\n"
        "[ 270] 2 > done persist 1 entries with first index 4 (status 0)\n"
        "           replicated index 4 -> send success result to 1\n"
        "[ 270] 3 > done persist 1 entries with first index 4 (status 0)\n"
        "           replicated index 4 -> send success result to 1\n"
        "[ 280] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 4\n");

    ASSERT_CONFIGURATION_INDEXES(1, 4, 0);

    return MUNIT_OK;
}

/* If a follower receives an AppendEntries RPC containing a RAFT_CHANGE entry
 * which changes the role of a server, the configuration change is immediately
 * applied locally, even if the entry is not yet committed. Once the entry is
 * committed, the change becomes permanent.*/
TEST(raft_assign, changeIsImmediate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_configuration conf;
    struct raft *raft;
    int rv;

    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 0, vote 0, no snapshot, no entries\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n");

    /* Submit a configuration change to promote server 3 to stand-by. */
    CLUSTER_POPULATE_CONFIGURATION(&conf, 3, 2, 1);

    entry.type = RAFT_CHANGE;
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&conf);

    raft = CLUSTER_GET(1);
    rv = raft_accept(raft, &entry, 1);
    munit_assert_int(rv, ==, 0);

    /* Server 2 eventually receives and persists the entry containing the new
     * configuration. */
    CLUSTER_TRACE(
        "[ 130] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 3 sending 2 entries with first index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 140] 3 > recv append entries from server 1\n"
        "           remote term is higher (2 vs 0) -> bump term\n"
        "           no entry at index 1 -> reject\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n"
        "           pipeline server 2 sending 1 entries with first index 3\n"
        "[ 150] 1 > recv append entries result from server 3\n"
        "           log mismatch -> send old entries to 3\n"
        "           probe server 3 sending 3 entries with first index 1\n"
        "[ 160] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 160] 3 > recv append entries from server 1\n"
        "           persist 3 entries with first index 1\n"
        "[ 160] 1 > tick\n"
        "[ 170] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 170] 3 > done persist 3 entries with first index 1 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n");

    /* The uncommitted configuration takes effect immediately. */
    ASSERT_CONFIGURATION_INDEXES(2, 1, 3);

    return MUNIT_OK;
}

/* Assign the stand-by role to an idle server. */
TEST(raft_assign, promoteToStandBy, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    GROW;
    ADD(0, 3);
    ASSIGN(0, 3, RAFT_STANDBY);
    */
    return MUNIT_OK;
}

/* Trying to promote a server on a raft instance which is not the leader results
 * in an error. */
TEST(raft_assign, notLeader, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    ASSIGN_ERROR(1, 3, RAFT_VOTER, RAFT_NOTLEADER, "server is not the leader");
    */
    return MUNIT_OK;
}

/* Trying to change the role of a server whose ID is unknown results in an
 * error. */
TEST(raft_assign, unknownId, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    ASSIGN_ERROR(0, 3, RAFT_VOTER, RAFT_NOTFOUND, "no server has ID 3");
    */
    return MUNIT_OK;
}

/* Trying to promote a server to an unknown role in an. */
TEST(raft_assign, badRole, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    ASSIGN_ERROR(0, 3, 999, RAFT_BADROLE, "server role is not valid");
    */
    return MUNIT_OK;
}

/* Trying to assign the voter role to a server which has already it results in
 * an error. */
TEST(raft_assign, alreadyHasRole, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    ASSIGN_ERROR(0, 1, RAFT_VOTER, RAFT_BADROLE, "server is already voter");
    */
    return MUNIT_OK;
}

/* Trying to assign a new role to a server while a configuration change is in
 * progress results in an error. */
TEST(raft_assign, changeRequestAlreadyInProgress, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_configuration conf;
    struct raft *raft;
    int rv;

    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 0, vote 0, no snapshot, no entries\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n");

    /* Submit a configuration change to promote server 3 to voter. */
    CLUSTER_POPULATE_CONFIGURATION(&conf, 3, 3, 0);

    entry.type = RAFT_CHANGE;
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&conf);

    raft = CLUSTER_GET(1);
    rv = raft_accept(raft, &entry, 1);
    munit_assert_int(rv, ==, 0);

    /* Attempt to submit a configuration change to add a fourth server. */
    CLUSTER_POPULATE_CONFIGURATION(&conf, 4, 3, 0);

    entry.type = RAFT_CHANGE;
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&conf);

    raft = CLUSTER_GET(1);
    rv = raft_accept(raft, &entry, 1);
    munit_assert_int(rv, ==, RAFT_CANTCHANGE);

    raft_free(entry.buf.base);

    return MUNIT_OK;
}

/* If leadership is lost before the configuration change log entry for setting
 * the new server role is committed, the leader configuration gets rolled back
 * and the role of server being changed is reverted. */
TEST(raft_assign, leadershipLost, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    const struct raft_server *server;
    */
    /* TODO: fix */
    return MUNIT_SKIP;
    /*
    GROW;
    ADD(0, 3);
    CLUSTER_STEP_N(2);

    ASSIGN_SUBMIT(0, 3, RAFT_VOTER);
    */

    /* Server 3 is being considered as voting, even though the configuration
     * change is not committed yet. */
    /*
    ASSERT_CATCH_UP_ROUND(0, 0, 0, 0);
    ASSERT_CONFIGURATION_INDEXES(0, 2, 3);
    server = configurationGet(&CLUSTER_RAFT(0)->configuration, 3);
    munit_assert_int(server->role, ==, RAFT_VOTER);
    */

    /* Lose leadership. */
    // CLUSTER_DEPOSE;

    /* A new leader gets elected */
    // CLUSTER_ELECT(1);
    // CLUSTER_STEP_N(5);

    /* Server 3 is not being considered voting anymore. */
    // server = configurationGet(&CLUSTER_RAFT(0)->configuration, 3);
    // munit_assert_int(server->role, ==, RAFT_STANDBY);

    return MUNIT_OK;
}

/* Trying to assign the voter role to an unresponsive server eventually
 * fails. */
TEST(raft_assign, promoteUnresponsive, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    struct raft_configuration conf;
    struct raft_entry entry;
    int rv;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 1 /* N voting */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "           self elect and convert to leader\n"
        "[   0] 2 > term 0, vote 0, no snapshot, no entries\n");

    /* Submit the configuration change request. */
    CLUSTER_POPULATE_CONFIGURATION(&conf, 2, 2, 0);

    entry.type = RAFT_CHANGE;
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&conf);

    raft = CLUSTER_GET(1);
    rv = raft_accept(raft, &entry, 1);
    munit_assert_int(rv, ==, 0);

    /* Make server 2 unresponsive and lower the max catch round duration setting
     * on server 1, so it will consider server 1 unresponsive earlier. */
    CLUSTER_KILL(2);
    raft_set_max_catch_up_round_duration(raft, 110);

    munit_assert_int(raft->leader_state.promotee_id, ==, 2);

    CLUSTER_TRACE(
        "[   0] 1 > accept 1 commands starting at 2\n"
        "           probe server 2 sending 0 entries with first index 2\n"
        "[ 100] 1 > tick\n"
        "           probe server 2 sending 0 entries with first index 2\n"
        "[ 140] 1 > tick\n"
        "           probe server 2 sending 0 entries with first index 2\n");

    munit_assert_int(raft->leader_state.promotee_id, ==, 0);

    return MUNIT_OK;
}

/* The leader can be demoted to stand-by and still act as leader, although it
 * won't have any voting right. */
TEST(raft_assign, demoteLeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    struct raft_configuration conf;
    struct raft_entry entry;
    int rv;

    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader and replicates the first no-op entry. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n");

    raft_configuration_init(&conf);
    rv = raft_configuration_add(&conf, 1, "1", RAFT_STANDBY);
    munit_assert_int(rv, ==, 0);
    rv = raft_configuration_add(&conf, 2, "2", RAFT_VOTER);

    munit_assert_int(rv, ==, 0);
    entry.type = RAFT_CHANGE;
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&conf);

    rv = raft_accept(CLUSTER_GET(1), &entry, 1);
    munit_assert_int(rv, ==, 0);

    CLUSTER_TRACE(
        "[ 150] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "           pipeline server 2 sending 1 entries with first index 3\n"
        "[ 160] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 160] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 160] 1 > tick\n"
        "[ 170] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 180] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 3\n");

    raft = CLUSTER_GET(1);
    munit_assert_int(raft_state(raft), ==, RAFT_LEADER);

    munit_assert_int(raft->configuration_index, ==, 3);
    munit_assert_int(raft->configuration.servers[0].role, ==, RAFT_STANDBY);

    return MUNIT_OK;
}
