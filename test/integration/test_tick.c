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

/* Assert the current value of the timer of the server with the given. ID */
#define ASSERT_ELECTION_TIMER(ID, MSECS)                                       \
    {                                                                          \
        struct raft *_raft = CLUSTER_GET(ID);                                  \
        munit_assert_int(                                                      \
            _raft->clock->now(_raft->clock) - _raft->election_timer_start, ==, \
            MSECS);                                                            \
    }

SUITE(tick)

/* Internal timers are updated as time elapses. */
TEST(tick, electionTimer, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 3 /* N servers */, 3 /* N voting */);

    CLUSTER_START(1);
    CLUSTER_START(2);
    CLUSTER_START(3);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n");

    ASSERT_ELECTION_TIMER(1, 0);
    ASSERT_ELECTION_TIMER(2, 0);
    ASSERT_ELECTION_TIMER(3, 0);

    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    ASSERT_ELECTION_TIMER(1, 0);
    ASSERT_ELECTION_TIMER(2, 100);
    ASSERT_ELECTION_TIMER(3, 100);

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    ASSERT_ELECTION_TIMER(1, 10);
    ASSERT_ELECTION_TIMER(2, 0);
    ASSERT_ELECTION_TIMER(3, 0);

    return MUNIT_OK;
}

/* If the election timeout expires, the follower is a voting server, and it
 * hasn't voted yet in this term, then become candidate and start a new
 * election. */
TEST(tick, candidate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    CLUSTER_GROW(2);

    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader */
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

    /* Kill server 1, eventually server 2 converts to candidate. */
    CLUSTER_KILL(1);

    CLUSTER_TRACE(
        "[ 240] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n");

    raft = CLUSTER_GET(2);

    /* The term has been incremeted. */
    munit_assert_int(raft->current_term, ==, 3);

    /* We have voted for ouselves. */
    munit_assert_int(raft->voted_for, ==, 2);

    /* We are candidate */
    munit_assert_int(raft_state(raft), ==, RAFT_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(raft->candidate_state.votes);
    munit_assert_false(raft->candidate_state.votes[0]);
    munit_assert_true(raft->candidate_state.votes[1]);

    return MUNIT_OK;
}

/* If the election timeout has elapsed, but we're not voters, stay follower. */
TEST(tick, notVoter, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    // struct raft *raft;
    CLUSTER_GROW(2);

    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 1 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 1 /* N voting */);

    /* Server 1 self-elects itself. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "           self elect and convert to leader\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n");

    /* Kill server 1, server 2 stays follower when the election timeout
     * expires. */
    CLUSTER_KILL(1);

    munit_assert_int(raft_state(CLUSTER_GET(2)), ==, RAFT_FOLLOWER);

    return MUNIT_OK;
}

/* If we're leader election timeout elapses without hearing from a majority of
 * the cluster, step down. */
TEST(tick, noContact, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);

    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader */
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

    /* Kill server 2, eventually server 1 steps down. */
    CLUSTER_KILL(2);
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 160] 1 > tick\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "[ 200] 1 > tick\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "[ 240] 1 > tick\n"
        "           unable to contact majority of cluster -> step down\n");

    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_FOLLOWER);

    return MUNIT_OK;
}

/* If we're candidate and the election timeout has elapsed, start a new
 * election. */
TEST(tick, newElection, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    CLUSTER_GROW(2);

    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Become candidate */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* Kill server 2 so server 1 can't win the election. */
    CLUSTER_KILL(2);

    CLUSTER_TRACE(
        "[ 270] 1 > tick\n"
        "           stay candidate and start new election for term 3\n");

    raft = CLUSTER_GET(1);

    /* The term has been incremeted and saved to stable store. */
    munit_assert_int(raft->current_term, ==, 3);

    /* We have voted for ouselves. */
    munit_assert_int(raft->voted_for, ==, 1);

    /* We are still candidate */
    munit_assert_int(raft_state(raft), ==, RAFT_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(raft->candidate_state.votes);
    munit_assert_true(raft->candidate_state.votes[0]);
    munit_assert_false(raft->candidate_state.votes[1]);

    return MUNIT_OK;
}
