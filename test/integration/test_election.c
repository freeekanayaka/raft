#include "../../include/raft.h"
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

SUITE(election)

/* Test an election round with two voters. */
TEST(election, twoVoters, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n");

    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n");

    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_LEADER);

    return MUNIT_OK;
}

/* If we have already voted and the same candidate requests the vote again, the
 * vote is granted. */
TEST(election, grantAgain, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Prevent server 2 from timing out. */
    raft_set_election_timeout(CLUSTER_GET(2), 10000);
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n");

    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* Disconnect the second server, so the first server does not receive the
     * result and eventually starts a new election round. */
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_TRACE(
        "[ 270] 1 > tick\n"
        "           stay candidate and start new election for term 3\n");

    /* Reconnecting the two servers eventually makes the first server win the
     * election. */
    CLUSTER_RECONNECT(2, 1);

    CLUSTER_TRACE(
        "[ 280] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    CLUSTER_TRACE(
        "[ 290] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n");

    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_LEADER);

    return MUNIT_OK;
}

/* If the requester last log entry index is the same, the vote is granted. */
TEST(election, grantIfLastIndexIsSame, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    CLUSTER_PERSIST_COMMAND(1 /*ID */, 1 /* term */, 1 /* payload */);
    CLUSTER_PERSIST_COMMAND(2 /*ID */, 1 /* term */, 1 /* payload */);
    CLUSTER_PERSIST_TERM(1 /* ID */, 2 /* term */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 2, vote 0, no snapshot, entries 1/1 to 2/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 2/1\n");

    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 3\n");

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log equal or longer (2/1 vs 2/1) -> grant vote\n");

    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 2 sending 1 entries with first index 3\n");

    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_LEADER);

    return MUNIT_OK;
}

/* If the requester last log entry index is higher, the vote is granted. */
TEST(election, grantIfLastIndexIsHigher, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    CLUSTER_PERSIST_TERM(1 /* ID */, 2 /* term */);
    CLUSTER_PERSIST_COMMAND(1 /*ID */, 1 /* term */, 1 /* payload */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 2, vote 0, no snapshot, entries 1/1 to 2/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n");

    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 3\n");

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log equal or longer (2/1 vs 1/1) -> grant vote\n");

    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 2 sending 1 entries with first index 3\n");

    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_LEADER);

    return MUNIT_OK;
}

/* If a candidate receives a vote request response granting the vote but the
 * quorum is not reached, it stays candidate. */
TEST(election, waitQuorum, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(5);
    CLUSTER_BOOTSTRAP(1 /* ID */, 5 /* N servers */, 5 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 5 /* N servers */, 5 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 5 /* N servers */, 5 /* N voting */);
    CLUSTER_BOOTSTRAP(4 /* ID */, 5 /* N servers */, 5 /* N voting */);
    CLUSTER_BOOTSTRAP(5 /* ID */, 5 /* N servers */, 5 /* N voting */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_START(4 /* ID */);
    CLUSTER_START(5 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 4 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 5 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n");

    /* The first server converts to candidate and sends vote requests. */
    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* All servers receive the request, grant their vote and send the reply. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 110] 4 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[ 110] 5 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* The first server receives the first RequestVote result RPC but stays
     * candidate since it has only 2 votes, and 3 are required. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum not reached\n");

    /* The first server receives the second RequestVote result RPC and converst
     * to leader. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 3\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "           probe server 3 sending 1 entries with first index 2\n"
        "           probe server 4 sending 1 entries with first index 2\n"
        "           probe server 5 sending 1 entries with first index 2\n");

    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_LEADER);

    return MUNIT_OK;
}

/* The vote request gets rejected if the remote term is lower than ours. */
TEST(election, rejectIfTermIsLower, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    CLUSTER_PERSIST_TERM(2 /* ID */, 3 /* term */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 3, vote 0, no snapshot, entries 1/1 to 1/1\n");

    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");
    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_CANDIDATE);

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is lower (2 vs 3) -> reject\n");

    /* The first server receives the RequestVote result RPC and converts to
     * follower because it discovers the newer term. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n");

    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_FOLLOWER);

    return 0;
}

/* If the server already has a leader, the vote is not granted (even if the
 * request has a higher term). */
TEST(election, rejectIfHasLeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 3 /* N servers */, 3 /* N voting */);

    /* Server 1 wins the elections. */
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

    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_LEADER);

    /* Server 2 gets disconnected and becomes candidate. */
    CLUSTER_DISCONNECT(1, 2);
    raft_seed(CLUSTER_GET(2), 170);
    raft_set_election_timeout(CLUSTER_GET(2), 30);
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n"
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 3 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 140] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n");

    munit_assert_int(raft_state(CLUSTER_GET(2)), ==, RAFT_CANDIDATE);

    /* Server 2 stays candidate since its requests get rejected. */
    CLUSTER_TRACE(
        "[ 150] 1 > recv append entries result from server 3\n"
        "           replication quorum reached -> commit index 2\n"
        "[ 150] 1 > recv request vote from server 2\n"
        "           local server has a leader -> reject\n"
        "[ 150] 3 > recv request vote from server 2\n"
        "           local server has a leader -> reject\n"
        "[ 160] 2 > recv request vote result from server 3\n"
        "           remote term is lower (2 vs 3) -> ignore\n");

    munit_assert_int(raft_state(CLUSTER_GET(2)), ==, RAFT_CANDIDATE);

    return MUNIT_OK;
}

/* If a server has already voted, vote is not granted. */
TEST(election, rejectIfAlreadyVoted, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 3 /* N servers */, 3 /* N voting */);

    /* Disconnect server 2 from server 1 and change its randomized election
     * timeout to match the one of server 1. This way server 2 will convert to
     * candidate but not receive vote requests. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    raft_seed(CLUSTER_GET(2), 13);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n");

    /* Server 1 and server 2 both become candidates. */
    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n"
        "[ 100] 2 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* Server 3 receives the vote request from server 1 and grants it. */
    CLUSTER_TRACE(
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* Server 2 receives the vote request from server 1 and rejects it because
     * it has already voted. */
    CLUSTER_TRACE(
        "[ 110] 3 > recv request vote from server 2\n"
        "           local server already voted for 1 -> don't grant vote\n");

    /* Server 1 receives the vote result from server 2 and becomes leader. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 3\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "           probe server 3 sending 1 entries with first index 2\n");

    /* Server 2 is still candidate because its vote request got rejected. */
    CLUSTER_TRACE(
        "[ 120] 2 > recv request vote result from server 3\n"
        "           vote not granted\n");

    munit_assert_int(raft_state(CLUSTER_GET(2)), ==, RAFT_CANDIDATE);

    return MUNIT_OK;
}

/* If the requester last log entry term is lower than ours, the vote is not
 * granted. */
TEST(election, rejectIfLastTermIsLower, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    CLUSTER_PERSIST_COMMAND(1 /*ID */, 1 /* term */, 123 /* payload */);
    CLUSTER_PERSIST_COMMAND(2 /*ID */, 2 /* term */, 456 /* payload */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 2/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 2/2\n");

    /* The first server becomes candidate. */
    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log older (2/1 vs 2/2) -> don't grant vote\n");

    /* The first server receives the response and stays candidate. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           vote not granted\n");

    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_CANDIDATE);

    /* Eventually the second server becomes leader because it has a longer
     * log. */
    CLUSTER_TRACE(
        "[ 130] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 140] 1 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           remote log has higher term (2/2 vs 2/1) -> grant vote\n"
        "[ 150] 2 > recv request vote result from server 1\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 1 sending 1 entries with first index 3\n");

    munit_assert_int(raft_state(CLUSTER_GET(2)), ==, RAFT_LEADER);

    return MUNIT_OK;
}

/* If the requester last log entry index is the lower, the vote is not
 * granted. */
TEST(election, rejectIfLastIndexIsLower, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    CLUSTER_PERSIST_COMMAND(2 /*ID */, 1 /* term */, 123 /* payload */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 2/1\n");

    /* The first server becomes candidate. */
    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log shorter (1/1 vs 2/1) -> don't grant vote\n");

    /* The first server receives the response and stays candidate. */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           vote not granted\n");

    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_CANDIDATE);

    /* Eventually the second server becomes leader because it has a longer
     * log. */
    CLUSTER_TRACE(
        "[ 130] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 140] 1 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           remote log equal or longer (2/1 vs 1/1) -> grant vote\n"
        "[ 150] 2 > recv request vote result from server 1\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 1 sending 1 entries with first index 3\n");

    munit_assert_int(raft_state(CLUSTER_GET(2)), ==, RAFT_LEADER);

    return MUNIT_OK;
}

/* If we are not a voting server, the vote is not granted. */
TEST(election, rejectIfNotVoter, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    */
    /* Disconnect server 0 from server 1, so server 0 can't win the elections
     * (since there are only 2 voting servers). */
    /*
    CLUSTER_SATURATE_BOTHWAYS(0, 1);

    CLUSTER_START;
    */

    /* Server 0 becomes candidate. */
    /*
    STEP_UNTIL_CANDIDATE(0);
    ASSERT_TIME(1000);
    */

    /* Server 0 stays candidate because it can't reach a quorum. */
    /*
    CLUSTER_STEP_UNTIL_TERM_IS(0, 3, 2000);
    ASSERT_CANDIDATE(0);
    ASSERT_TIME(2000);
    */

    return MUNIT_OK;
}

/* Non-voting servers are skipped when sending vote requests. */
TEST(election, skipNonVoters, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 3 /* N servers */, 2 /* N voting */);

    /* Disconnect server 1 from server 2, so server 1 can't win the elections,
     * since it needs the vote from 2. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n");

    /* Server 1 becomes candidate. */
    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* Server 1 stays candidate because it can't reach a quorum and eventually
     * server 2 becomes candidate as well. */
    CLUSTER_TRACE(
        "[ 130] 2 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    munit_assert_int(raft_state(CLUSTER_GET(1)), ==, RAFT_CANDIDATE);
    munit_assert_int(raft_state(CLUSTER_GET(2)), ==, RAFT_CANDIDATE);

    return MUNIT_OK;
}

/* The I/O error occurs when sending a vote request, and gets ignored. */
TEST(election, ioErrorSendVoteRequest, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n");

    /* The first server will fail to send a RequestVote RPC. */
    CLUSTER_DISCONNECT(1, 2);

    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* The second server doesn't receive the request and converst to
     * candidate. */
    CLUSTER_TRACE(
        "[ 130] 2 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    return MUNIT_OK;
}

/* Test an election round with two voters and pre-vote. */
TEST(election, preVote, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    raft_set_pre_vote(CLUSTER_GET(1), true);
    raft_set_pre_vote(CLUSTER_GET(2), true);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* Server 1 has not incremented its term or persisted its vote.*/
    munit_assert_int(CLUSTER_GET(1)->current_term, ==, 1);
    munit_assert_int(CLUSTER_GET(1)->voted_for, ==, 0);

    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* Server 2 has not incremented its term or persisted its vote.*/
    munit_assert_int(CLUSTER_GET(2)->current_term, ==, 1);
    munit_assert_int(CLUSTER_GET(2)->voted_for, ==, 0);

    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> pre-vote successful\n");

    /* Server 1 has now incremented its term. */
    munit_assert_int(CLUSTER_GET(1)->current_term, ==, 2);

    CLUSTER_TRACE(
        "[ 130] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* Server 2 has incremented its term and persisted its vote. */
    munit_assert_int(CLUSTER_GET(2)->current_term, ==, 2);
    munit_assert_int(CLUSTER_GET(2)->voted_for, ==, 1);

    CLUSTER_TRACE(
        "[ 140] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n");

    return MUNIT_OK;
}

/* A candidate receives votes then crashes. */
TEST(election, preVoteWithcandidateCrash, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(3);
    raft_set_pre_vote(CLUSTER_GET(1), true);
    raft_set_pre_vote(CLUSTER_GET(2), true);
    raft_set_pre_vote(CLUSTER_GET(3), true);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 3 /* N servers */, 3 /* N voting */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n");

    /* Server 1 eventually times out and converts to candidate, but it does not
     * increment its term yet.*/
    CLUSTER_TRACE(
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");
    munit_assert_int(CLUSTER_GET(1)->current_term, ==, 1);

    /* Server 1 completes sending pre-vote RequestVote RPCs */

    /* Server 2 receives the pre-vote RequestVote RPC but does not increment its
     * term. */
    CLUSTER_TRACE(
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");
    munit_assert_int(CLUSTER_GET(2)->current_term, ==, 1);

    /* Server 3 receives the pre-vote RequestVote RPC but does not increment its
     * term. */
    CLUSTER_TRACE(
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");
    munit_assert_int(CLUSTER_GET(3)->current_term, ==, 1);

    /* Server 2 and 3 complete sending pre-vote RequestVote results */

    /* Server 1 receives the pre-vote RequestVote results */
    CLUSTER_TRACE(
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> pre-vote successful\n"
        "[ 120] 1 > recv request vote result from server 3\n"
        "           remote term is lower (1 vs 2) -> ignore\n");

    /* Server 1 completes sending actual RequestVote RPCs */

    /* Server 2 receives the actual RequestVote RPC */
    CLUSTER_TRACE(
        "[ 130] 2 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* Server 3 receives the actual RequestVote RPC */
    CLUSTER_TRACE(
        "[ 130] 3 > recv request vote from server 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* Server 1 crashes. */
    CLUSTER_KILL(1);

    /* Server 2 times out and starts an election. It doesn't increment its term
     * yet but it reset its vote since it's beginning the pre-vote phase. */
    CLUSTER_TRACE(
        "[ 260] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n");
    munit_assert_int(CLUSTER_GET(2)->current_term, ==, 2);
    munit_assert_int(CLUSTER_GET(2)->voted_for, ==, 0);

    /* Since server 3 has already voted for server 0, it doesn't grant its vote
     * and eventually times out and becomes candidate, resetting its vote as
     * well. */
    CLUSTER_TRACE(
        "[ 270] 3 > recv request vote from server 2\n"
        "           local server already voted for 1 -> don't grant vote\n");

    CLUSTER_TRACE(
        "[ 280] 2 > recv request vote result from server 3\n"
        "           vote not granted\n");

    CLUSTER_TRACE(
        "[ 290] 3 > tick\n"
        "           convert to candidate and start new election for term 3\n");
    munit_assert_int(CLUSTER_GET(3)->current_term, ==, 2);
    munit_assert_int(CLUSTER_GET(3)->voted_for, ==, 0);

    /* Server 3 completes sending the pre-vote RequestVote RPCs */

    /* Server 2 receives the pre-vote RequestVote RPC */
    CLUSTER_TRACE(
        "[ 300] 2 > recv request vote from server 3\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* Server 2 completes sending pre-vote RequestVote results */

    /* Server 3 receives the pre-vote RequestVote results */
    CLUSTER_TRACE(
        "[ 310] 3 > recv request vote result from server 2\n"
        "           votes quorum reached -> pre-vote successful\n");

    /* Server 2 completes sending actual RequestVote RPCs */

    /* Server 1 receives the actual RequestVote RPC */
    CLUSTER_TRACE(
        "[ 320] 2 > recv request vote from server 3\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* Server 2 completes sending actual RequestVote result */

    /* Server 3 receives the actual RequestVote result */
    CLUSTER_TRACE(
        "[ 330] 3 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 2\n"
        "           probe server 1 sending 1 entries with first index 2\n"
        "           probe server 2 sending 1 entries with first index 2\n");

    return MUNIT_OK;
}
