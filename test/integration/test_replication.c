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

SUITE(replication)

/* A leader replicates a blank no-op entry as soon as it gets elected. */
TEST(replication, sendInitialNoOpEntry, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n");

    /* Server 1 becomes candidate and sends vote requests after the election
     * timeout. */
    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* Server 1 receives the vote result, becomes leader and replicates
     * the initial no-op entry. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n");

    CLUSTER_STEP;
    raft = CLUSTER_GET(1);
    munit_assert_int(raft->leader_state.progress[1].last_send, ==, 120);

    /* Server 2 receives the heartbeat from server 1 and resets its election
     * timer. */
    raft = CLUSTER_GET(2);
    munit_assert_int(raft->election_timer_start, ==, 110);
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n");
    munit_assert_int(raft->election_timer_start, ==, 130);

    return MUNIT_OK;
}

/* A leader keeps sending heartbeat messages at regular intervals to
 * maintain leadership. */
TEST(replication, sendHeartbeat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader and replicates the initial no-op entry. */
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
        "           probe server 2 sending 1 entries with first index 2\n");

    /* Server 2 receives the first heartbeat. */
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n");
    raft = CLUSTER_GET(2);
    munit_assert_int(raft->election_timer_start, ==, 130);

    /* Server 2 receives an heartbeat. */
    CLUSTER_TRACE(
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n"
        "[ 160] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 3\n"
        "[ 170] 2 > recv append entries from server 1\n");
    munit_assert_int(raft->election_timer_start, ==, 170);

    return MUNIT_OK;
}

/* If a leader replicates some entries during a given heartbeat interval, it
 * skips sending the heartbeat for that interval. */
TEST(replication, skipHeartbeat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    struct raft_entry entry;
    int rv;

    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader and replicates the initial no-op entry. */
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

    raft = CLUSTER_GET(1);
    munit_assert_int(raft->leader_state.progress[1].last_send, ==, 120);

    /* Server 1 starts replicating a new entry after 5 milliseconds. When the
     * heartbeat timeout kicks in just after that, no heartbeat gets sent. */
    CLUSTER_ELAPSE(5);

    entry.type = RAFT_COMMAND;
    entry.buf.base = raft_malloc(8);
    entry.buf.len = 8;
    rv = raft_accept(raft, &entry, 1);
    munit_assert_int(rv, ==, 0);

    CLUSTER_TRACE(
        "[ 155] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "           pipeline server 2 sending 1 entries with first index 3\n"
        "[ 160] 1 > tick\n");

    munit_assert_int(raft->leader_state.progress[1].last_send, ==, 155);

    /* When the heartbeat timeout expires again, server 1 sends a fresh
     * heartbeat round, since no entries where sent since the last timeout. */
    CLUSTER_TRACE(
        "[ 165] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 165] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 175] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 185] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 3\n"
        "[ 200] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 4\n");

    CLUSTER_STEP;
    munit_assert_int(raft->leader_state.progress[1].last_send, ==, 200);

    return MUNIT_OK;
}

/* The leader doesn't send replication messages to idle servers. */
TEST(replication, skipIdle, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    struct raft_change req1;
    struct raft_apply req2;
    BOOTSTRAP_START_AND_ELECT;
    CLUSTER_ADD(&req1);
    CLUSTER_STEP_UNTIL_APPLIED(0, 2, 1000);
    CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, &req2, 1, NULL);
    CLUSTER_STEP_UNTIL_ELAPSED(1000);
    munit_assert_int(CLUSTER_LAST_APPLIED(0), ==, 3);
    munit_assert_int(CLUSTER_LAST_APPLIED(1), ==, 3);
    munit_assert_int(CLUSTER_LAST_APPLIED(2), ==, 0);
    */
    return MUNIT_OK;
}

/* A follower remains in probe mode until the leader receives a successful
 * AppendEntries response. */
TEST(replication, sendProbe, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    int rv;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader and replicates the initial no-op entry. */
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
        "           probe server 2 sending 1 entries with first index 2\n");

    /* Set a network latency higher than the heartbeat timeout for server 2, so
     * server 1 will send a second probe AppendEntries without transitioning to
     * pipeline mode. */
    CLUSTER_NETWORK_LATENCY(2, 70);

    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n");

    /* Server 1 receives a new entry after a few milliseconds. Since the
     * follower is still in probe mode and since an AppendEntries message was
     * already sent recently, it does not send the new entry immediately. */
    CLUSTER_ELAPSE(5);
    entry.type = RAFT_COMMAND;
    entry.buf.base = raft_malloc(8);
    entry.buf.len = 8;
    rv = raft_accept(CLUSTER_GET(1), &entry, 1);
    munit_assert_int(rv, ==, 0);
    CLUSTER_TRACE(
        "[ 135] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n");

    /* A heartbeat timeout elapses without receiving a response, so server 0
     * sends an new AppendEntries to server 1. This time it includes also the
     * new entry that was accepted in the meantime. */
    CLUSTER_TRACE(
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 145] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 160] 1 > tick\n"
        "           probe server 2 sending 2 entries with first index 2\n");

    /* Now lower the network latency of server 2, so the AppendEntries result
     * for this last AppendEntries request will get delivered before the
     * response of original AppendEntries request for the no-op entry . */
    CLUSTER_NETWORK_LATENCY(2, 10);

    /* Server 1 receives a second entry after a few milliseconds. Since the
     * follower is still in probe mode and since an AppendEntries message was
     * already sent recently, it does not send the new entry immediately. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_ELAPSE(5);
    entry.type = RAFT_COMMAND;
    entry.buf.base = raft_malloc(8);
    entry.buf.len = 8;
    rv = raft_accept(CLUSTER_GET(1), &entry, 1);
    munit_assert_int(rv, ==, 0);
    CLUSTER_TRACE(
        "[ 170] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 180] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 185] 1 > accept 1 commands starting at 4\n"
        "           persist 1 entries with first index 4\n");

    /* Eventually server 1 receives the AppendEntries result for the second
     * reuest, at that point it transitions to pipeline mode and sends
     * the second entry immediately. */
    CLUSTER_TRACE(
        "[ 190] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 3\n"
        "           pipeline server 2 sending 1 entries with first index 4\n");

    return MUNIT_OK;
}

/* A follower transitions to pipeline mode after the leader receives a
 * successful AppendEntries response from it. */
TEST(replication, sendPipeline, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    struct raft_entry entry;
    int rv;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader and replicates the initial no-op entry. */
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

    /* Server 1 receives a new entry after 5 milliseconds, just before the
     * heartbeat timeout expires. Since the follower has transitioned to
     * pipeline mode the new entry is sent immediately and the next index is
     * optimistically increased. */
    CLUSTER_ELAPSE(5);

    entry.type = RAFT_COMMAND;
    entry.buf.base = raft_malloc(8);
    entry.buf.len = 8;
    raft = CLUSTER_GET(1);
    rv = raft_accept(CLUSTER_GET(1), &entry, 1);
    munit_assert_int(rv, ==, 0);

    CLUSTER_TRACE(
        "[ 155] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "           pipeline server 2 sending 1 entries with first index 3\n");
    munit_assert_int(raft->leader_state.progress[1].next_index, ==, 4);

    /* After another 15 milliseconds, before receiving the response for this
     * last AppendEntries RPC and before the heartbeat timeout expires, server 1
     * accepts a second entry, which is also replicated immediately. */
    CLUSTER_TRACE(
        "[ 160] 1 > tick\n"
        "[ 165] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 165] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 175] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n");
    CLUSTER_STEP;
    CLUSTER_ELAPSE(5);
    entry.type = RAFT_COMMAND;
    entry.buf.base = raft_malloc(8);
    entry.buf.len = 8;
    raft = CLUSTER_GET(1);
    rv = raft_accept(CLUSTER_GET(1), &entry, 1);
    munit_assert_int(rv, ==, 0);
    CLUSTER_TRACE(
        "[ 180] 1 > accept 1 commands starting at 4\n"
        "           persist 1 entries with first index 4\n"
        "           pipeline server 2 sending 1 entries with first index 4\n");
    munit_assert_int(raft->leader_state.progress[1].next_index, ==, 5);

    /* Eventually server 0 receives AppendEntries results for both entries. */
    CLUSTER_TRACE(
        "[ 185] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 3\n"
        "[ 190] 1 > done persist 1 entries with first index 4 (status 0)\n"
        "           replication quorum not reached for index 4\n"
        "[ 190] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 4\n"
        "[ 200] 2 > done persist 1 entries with first index 4 (status 0)\n"
        "           replicated index 4 -> send success result to 1\n"
        "[ 200] 1 > tick\n"
        "[ 210] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 4\n");

    return MUNIT_OK;
}

/* A follower disconnects while in probe mode. */
TEST(replication, sendDisconnect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader and replicates the initial no-op entry, however
     * it fails because server 2 has disconnected. */
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
        "           probe server 2 sending 1 entries with first index 2\n");

    CLUSTER_DISCONNECT(1, 2);

    /* After the heartbeat timeout server 1 retries, but still fails. */
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 160] 1 > tick\n"
        "           probe server 2 sending 2 entries with first index 1\n");

    /* After another heartbeat timeout server 1 retries and this time
     * succeeds. */
    CLUSTER_TRACE(
        "[ 200] 1 > tick\n"
        "           probe server 2 sending 2 entries with first index 1\n");
    CLUSTER_RECONNECT(1, 2);

    CLUSTER_TRACE(
        "[ 210] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 220] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 230] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n");

    return MUNIT_OK;
}

/* A follower disconnects while in pipeline mode. */
TEST(replication, sendDisconnectPipeline, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    struct raft_entry entry;
    int rv;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader and sends the initial no-op entry plus a
     * heartbeat. */
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
        "           replication quorum reached -> commit index 2\n"
        "[ 160] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 3\n"
        "[ 170] 2 > recv append entries from server 1\n"
        "[ 180] 1 > recv append entries result from server 2\n");

    /* It then starts to replicate a few entries, however server 2 disconnects
     * before delivering results. */
    CLUSTER_ELAPSE(10);
    entry.type = RAFT_COMMAND;
    entry.buf.base = raft_malloc(8);
    entry.buf.len = 8;
    rv = raft_accept(CLUSTER_GET(1), &entry, 1);
    munit_assert_int(rv, ==, 0);

    entry.type = RAFT_COMMAND;
    entry.buf.base = raft_malloc(8);
    entry.buf.len = 8;
    rv = raft_accept(CLUSTER_GET(1), &entry, 1);
    munit_assert_int(rv, ==, 0);

    CLUSTER_TRACE(
        "[ 190] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "           pipeline server 2 sending 1 entries with first index 3\n"
        "[ 190] 1 > accept 1 commands starting at 4\n"
        "           persist 1 entries with first index 4\n"
        "           pipeline server 2 sending 1 entries with first index 4\n");

    raft = CLUSTER_GET(1);
    munit_assert_int(raft->leader_state.progress[1].next_index, ==, 5);

    CLUSTER_STEP;
    CLUSTER_STEP;

    CLUSTER_DISCONNECT(1, 2);

    /* A heartbeat timeout eventually kicks in, but sending the empty
     * AppendEntries message fails, transitioning server 2 back to probe
     * mode. */
    CLUSTER_TRACE(
        "[ 200] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 200] 1 > done persist 1 entries with first index 4 (status 0)\n"
        "           replication quorum not reached for index 4\n"
        "[ 200] 1 > tick\n"
        "[ 240] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 5\n"
        "[ 280] 1 > tick\n"
        "           probe server 2 sending 2 entries with first index 3\n");
    munit_assert_int(raft->leader_state.progress[1].next_index, ==, 3);

    /* After reconnection the follower eventually replicates the entries and
     * reports back. */
    CLUSTER_RECONNECT(1, 2);
    CLUSTER_TRACE(
        "[ 290] 2 > recv append entries from server 1\n"
        "           persist 2 entries with first index 3\n"
        "[ 300] 2 > done persist 2 entries with first index 3 (status 0)\n"
        "           replicated index 4 -> send success result to 1\n"
        "[ 310] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 4\n");

    return MUNIT_OK;
}

static char *send_oom_heap_fault_delay[] = {"5", NULL};
static char *send_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum send_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, send_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, send_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory failures. */
TEST(replication, sendOom, setUp, tearDown, 0, send_oom_params)
{
    /*
    struct fixture *f = data;
    return MUNIT_SKIP;
    struct raft_apply req;
    BOOTSTRAP_START_AND_ELECT;

    HEAP_FAULT_ENABLE;

    CLUSTER_APPLY_ADD_X(0, &req, 1, NULL);
    */

    return MUNIT_OK;
}

/* A failure occurs upon submitting the I/O request. */
TEST(replication, sendIoError, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    return MUNIT_SKIP;
    struct raft_apply req;
    BOOTSTRAP_START_AND_ELECT;

    CLUSTER_IO_FAULT(0, 1, 1);

    CLUSTER_APPLY_ADD_X(0, &req, 1, NULL);
    */

    return MUNIT_OK;
}

/* Receive the same entry a second time, before the first has been persisted. */
TEST(replication, recvTwice, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 wins the election and replicates a no-op entry to server 2,
     * which receives it. */
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
        "           persist 1 entries with first index 2\n");

    /* Set a high disk latency for server 2, so the operation takes a long
     * time. Since replication for server 2 is still in the probe state, server
     * 1 eventually sends again the same entry. Server 2 receives it, but it
     * doesn't persist it again to disk, since the first persist request is
     * still in flight. */
    CLUSTER_DISK_LATENCY(2, 60);

    CLUSTER_TRACE(
        "[ 160] 1 > tick\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "[ 170] 2 > recv append entries from server 1\n");

    /* Eventually the original persist entries request from server 2 succeeds,
     * and is reported back to server 1. */
    CLUSTER_TRACE(
        "[ 180] 1 > recv append entries result from server 2\n"
        "           replicated index 1 lower or equal than commit index 1\n"
        "           pipeline server 2 sending 1 entries with first index 2\n"
        "[ 190] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 190] 2 > recv append entries from server 1\n"
        "[ 200] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n");

    return MUNIT_OK;
}

/* If the term in the request is stale, the server rejects it. */
TEST(replication, recvStaleTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 3 /* N servers */, 3 /* N voting */);

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
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "           probe server 3 sending 1 entries with first index 2\n");

    /* Partition server 1 from the other two and set a very high election
     * timeout on it, so it will keep sending heartbeats. */
    raft_set_election_timeout(CLUSTER_GET(1), 5000);
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 1);

    /* Server 2 eventually times out and starts an election. */
    raft_seed(CLUSTER_GET(2), 170);
    raft_set_election_timeout(CLUSTER_GET(2), 30);
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 140] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n");

    /* Eventually server 2 gets elected and server 1 sends a new heartbeat. */
    CLUSTER_TRACE(
        "[ 150] 3 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 160] 2 > recv request vote result from server 3\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 1 sending 1 entries with first index 2\n"
        "           probe server 3 sending 1 entries with first index 2\n"
        "[ 160] 1 > tick\n"
        "           probe server 2 sending 2 entries with first index 1\n"
        "           probe server 3 sending 2 entries with first index 1\n");

    /* Reconnect server 1 with server 3, which is the current follower. Server 3
     * receives the heartbeat and rejects it. */
    CLUSTER_RECONNECT(1, 3);
    CLUSTER_RECONNECT(3, 1);
    CLUSTER_TRACE(
        "[ 170] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 170] 3 > recv append entries from server 2\n"
        "           persist 1 entries with first index 2\n"
        "[ 170] 3 > recv append entries from server 1\n"
        "           local term is higher -> reject \n");

    /* Server 1 receives the reject message and steps down. */
    CLUSTER_TRACE(
        "[ 180] 3 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 2\n"
        "[ 180] 1 > recv append entries result from server 3\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n");

    return MUNIT_OK;
}

/* If server's log is shorter than prevLogIndex, the request is rejected . */
TEST(replication, recvMissingEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 has an entry that server 2 doesn't have */
    CLUSTER_PERSIST_COMMAND(1, 1, 123);

    /* Server 1 wins the election because it has a longer log. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 2/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (2/1 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 2 sending 1 entries with first index 3\n");

    /* Server 1 replicates a no-op entry to server 2, which initially rejects
     * it, because it's missing the one before. */
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no entry at index 2 -> reject\n");

    /* Server 1 sends the missing entry. */
    CLUSTER_TRACE(
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries to 2\n"
        "           probe server 2 sending 2 entries with first index 2\n");

    CLUSTER_TRACE(
        "[ 150] 2 > recv append entries from server 1\n"
        "           persist 2 entries with first index 2\n"
        "[ 160] 2 > done persist 2 entries with first index 2 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 160] 1 > tick\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 3\n");

    return MUNIT_OK;
}

/* If the term of the last log entry on the server is different from the one
 * prevLogTerm, and value of prevLogIndex is greater than server's commit index
 * (i.e. this is a normal inconsistency), we reject the request. */
TEST(replication, recvPrevLogTermMismatch, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* The servers have an entry with a conflicting term. */
    CLUSTER_PERSIST_COMMAND(1, 2, 123);
    CLUSTER_PERSIST_COMMAND(2, 1, 456);

    /* Server 1 becomes leader because its last entry has a higher term. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 2/2\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 2/1\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log has higher term (2/2 vs 2/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 2 sending 1 entries with first index 3\n");

    /* Server 2 rejects the initial AppendEntries request from server 1. */
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           previous term mismatch -> reject\n");

    /* Server 1 overwrites server's log. */
    CLUSTER_TRACE(
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries to 2\n"
        "           probe server 2 sending 2 entries with first index 2\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           local log has index 2 at term 1, remote at 2 -> truncate\n"
        "           persist 2 entries with first index 2\n"
        "[ 160] 2 > done persist 2 entries with first index 2 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 160] 1 > tick\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 3\n");

    return MUNIT_OK;
}

/* If any of the new entries has the same index of an existing entry in our log,
 * but different term, and that entry index is already committed, we bail out
 * with an error. */
TEST(replication, recvPrevIndexConflict, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* The servers have an entry with a conflicting term. */
    CLUSTER_PERSIST_COMMAND(1, 2, 123);
    CLUSTER_PERSIST_COMMAND(2, 1, 456);

    /* Server 1 becomes leader because its last entry has a higher term. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 2/2\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 2/1\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log has higher term (2/2 vs 2/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 2 sending 1 entries with first index 3\n");

    /* Artificially bump the commit index on the second server */
    CLUSTER_GET(2)->commit_index = 2;

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_FAIL(RAFT_SHUTDOWN);

    return MUNIT_OK;
}

/* A write log request is submitted for outstanding log entries. If some entries
 * are already existing in the log, they will be skipped. */
TEST(replication, recvSkip, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof *req);
    BOOTSTRAP_START_AND_ELECT;

    */
    /* Submit an entry */
    /*
    CLUSTER_APPLY_ADD_X(0, req, 1, NULL);

    */
    /* The leader replicates the entry to the follower however it does not get
     * notified about the result, so it sends the entry again. */
    /*
    CLUSTER_SATURATE_BOTHWAYS(0, 1);
    CLUSTER_STEP_UNTIL_ELAPSED(150);

    */
    /* The follower reconnects and receives again the same entry. This time the
     * leader receives the notification. */
    /*
    CLUSTER_DESATURATE_BOTHWAYS(0, 1);
    CLUSTER_STEP_UNTIL_APPLIED(0, req->index, 2000);

    free(req);
    */

    return MUNIT_OK;
}

/* If the index and term of the last snapshot on the server matches prevLogIndex
 * and prevLogTerm the request is accepted. */
TEST(replication, recvMatchLastSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);

    /* The first server has entry 2 */
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_PERSIST_COMMAND(1 /* ID */, 2 /* term */, 123 /* payload */);

    /* The second server has a snapshot up to entry 2 */
    CLUSTER_PERSIST_TERM(2 /* ID */, 1 /* term */);
    CLUSTER_PERSIST_SNAPSHOT(2 /* ID */, 2 /* last index */, 2 /* last term */,
                             2 /* N servers */, 2 /* N voting */,
                             1 /* conf index */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 2/2\n"
        "[   0] 2 > term 1, vote 0, snapshot 2/2, no entries\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (2/2 vs 2/2) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 2 sending 1 entries with first index 3\n");

    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 140] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 3\n");

    return MUNIT_OK;
}

/* If a candidate server receives a request contaning the same term as its
 * own, it it steps down to follower and accept the request . */
TEST(replication, recvCandidateSameTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 3 /* N servers */, 3 /* N voting */);

    /* Disconnect server 3 from the other two and set a low election timeout on
     * it, so it will immediately start an election. */
    CLUSTER_DISCONNECT(3, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 2);
    CLUSTER_DISCONNECT(2, 3);
    raft_set_election_timeout(CLUSTER_GET(3), 50);

    /* Server 3 becomes candidate. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[  90] 3 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* Server 1 wins the election and replicates a no-op entry. */
    CLUSTER_TRACE(
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

    /* Now reconnect server 3, which eventually steps down and replicates the
     * entry. */
    CLUSTER_RECONNECT(3, 1);
    CLUSTER_RECONNECT(1, 3);

    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           discovered leader -> step down \n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 140] 3 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n");

    return MUNIT_OK;
}

/* If a candidate server receives an append entries request contaning an higher
 * term than its own, it it steps down to follower and accept the request. */
TEST(replication, recvCandidateHigherTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 3 /* N servers */, 3 /* N voting */);

    /* Set a high election timeout on server 2, so it won't become candidate */
    raft_set_election_timeout(CLUSTER_GET(2), 500);

    /* Disconnect server 3 from the other two. */
    CLUSTER_DISCONNECT(3, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 2);
    CLUSTER_DISCONNECT(2, 3);

    /* Set a low election timeout on server 1, and disconnect it from server 2,
     * so by the time it wins the second round, server 3 will have turned
     * candidate */
    raft_set_election_timeout(CLUSTER_GET(1), 50);
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);

    /* Server 3 becomes candidate, and server 1 already is candidate. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[  97] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 160] 3 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* Server 1 starts a new election, while server 3 is still candidate */
    CLUSTER_TRACE(
        "[ 161] 1 > tick\n"
        "           stay candidate and start new election for term 3\n");

    /* Reconnect the server 1 and server 2, let the election succeed and
     * replicate an entry. */
    CLUSTER_RECONNECT(1, 2);
    CLUSTER_RECONNECT(2, 1);

    CLUSTER_TRACE(
        "[ 171] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 181] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "           probe server 3 sending 1 entries with first index 2\n"
        "[ 191] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 191] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 201] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 211] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n");

    /* Now reconnect the server 3, which eventually steps down and
     * replicates the entry. */
    CLUSTER_RECONNECT(3, 1);
    CLUSTER_RECONNECT(1, 3);
    CLUSTER_TRACE(
        "[ 221] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 3\n"
        "           probe server 3 sending 2 entries with first index 1\n"
        "[ 231] 2 > recv append entries from server 1\n"
        "[ 231] 3 > recv append entries from server 1\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           persist 1 entries with first index 2\n"
        "[ 241] 3 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n");

    return MUNIT_OK;
}

/* If the server handling the response is not the leader, the result
 * is ignored. */
TEST(replication, resultNotLeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader. */
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
        "           probe server 2 sending 1 entries with first index 2\n");

    /* Set a very high-latency for server 2's outgoing messages, so the first
     * server won't get notified about the results for a while. */
    CLUSTER_NETWORK_LATENCY(2, 100);

    /* Set a low election timeout on server 1 so it will step down very soon. */
    raft_set_election_timeout(CLUSTER_GET(1), 30);

    /* Eventually server 1 steps down becomes candidate. */
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 160] 1 > tick\n"
        "           unable to contact majority of cluster -> step down\n"
        "[ 205] 1 > tick\n"
        "           convert to candidate and start new election for term 3\n");

    /* The AppendEntries result eventually gets delivered, but the candidate
     * ignores it. */
    CLUSTER_TRACE(
        "[ 215] 2 > recv request vote from server 1\n"
        "           local server has a leader -> reject\n"
        "[ 240] 1 > recv append entries result from server 2\n");

    return MUNIT_OK;
}

/* If the response has a term which is lower than the server's one, it's
 * ignored. */
TEST(replication, resultLowerTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 3 /* N servers */, 3 /* N voting */);

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
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "           probe server 3 sending 1 entries with first index 2\n");

    /* Set a very high-latency for the server 2's outgoing messages, so server 1
     * won't get notified about the results for a while. */
    CLUSTER_NETWORK_LATENCY(2, 100);

    /* Set a high election timeout on server 2, so it won't become candidate */
    raft_set_election_timeout(CLUSTER_GET(2), 500);

    /* Disconnect server 1 from server 3 and set a low election timeout on it so
     * it will step down very soon. */
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 1);
    raft_set_election_timeout(CLUSTER_GET(1), 20);

    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 160] 1 > tick\n"
        "           unable to contact majority of cluster -> step down\n");

    /* Make server 0 become leader again. */
    CLUSTER_RECONNECT(1, 3);
    CLUSTER_RECONNECT(3, 1);
    CLUSTER_TRACE(
        "[ 200] 1 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 210] 2 > recv request vote from server 1\n"
        "           local server has a leader -> reject\n"
        "[ 210] 3 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log has higher term (2/2 vs 1/1) -> grant vote\n"
        "[ 220] 1 > recv request vote result from server 3\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 2 sending 1 entries with first index 3\n"
        "           probe server 3 sending 1 entries with first index 3\n");

    /* Eventually deliver the result message. */
    return 0;
    CLUSTER_TRACE(
        "[ 230] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 230] 2 > recv append entries from server 1\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           persist 1 entries with first index 3\n"
        "[ 230] 3 > recv append entries from server 1\n"
        "           no entry at index 2 -> reject\n"
        "[ 240] 1 > recv append entries result from server 2\n");

    return MUNIT_OK;
}

/* If the response has a term which is higher than the server's one, step down
 * to follower. */
TEST(replication, resultHigherTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 3 /* N servers */, 3 /* N voting */);

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
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "           probe server 3 sending 1 entries with first index 2\n");

    /* Set a very high election timeout for server 1 so it won't step down. */
    raft_set_election_timeout(CLUSTER_GET(1), 500);

    /* Disconnect the server 1 from the rest of the cluster. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 1);

    /* Eventually a new leader gets electected */
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 160] 1 > tick\n"
        "           probe server 2 sending 2 entries with first index 1\n"
        "           probe server 3 sending 2 entries with first index 1\n"
        "[ 200] 1 > tick\n"
        "           probe server 2 sending 2 entries with first index 1\n"
        "           probe server 3 sending 2 entries with first index 1\n"
        "[ 240] 1 > tick\n"
        "           probe server 2 sending 2 entries with first index 1\n"
        "           probe server 3 sending 2 entries with first index 1\n"
        "[ 240] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 250] 3 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 260] 2 > recv request vote result from server 3\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 1 sending 1 entries with first index 2\n"
        "           probe server 3 sending 1 entries with first index 2\n");

    /* Reconnect the old leader server 1 to the current follower server 3, which
     * eventually replies with an AppendEntries result containing an higher
     * term. */
    CLUSTER_RECONNECT(1, 3);
    CLUSTER_RECONNECT(3, 1);
    CLUSTER_TRACE(
        "[ 270] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 270] 3 > recv append entries from server 2\n"
        "           persist 1 entries with first index 2\n"
        "[ 280] 3 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 2\n"
        "[ 280] 1 > tick\n"
        "           probe server 2 sending 2 entries with first index 1\n"
        "           probe server 3 sending 2 entries with first index 1\n"
        "[ 290] 2 > recv append entries result from server 3\n"
        "           replication quorum reached -> commit index 2\n"
        "[ 290] 3 > recv append entries from server 1\n"
        "           local term is higher -> reject \n"
        "[ 300] 1 > recv append entries result from server 3\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n");

    return MUNIT_OK;
}
