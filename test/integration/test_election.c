#include "../../include/raft.h"
#include "../lib/cluster.h"
#include "../lib/runner.h"

struct fixture
{
    FIXTURE_CLUSTER;
};

static void *setUp(MUNIT_UNUSED const MunitParameter params[],
                   MUNIT_UNUSED void *user_data)
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
        "[   0] 1 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 2 > start - T:1 V:0 S:0/0 L:1/1\n");

    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1149] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1164] 2 > recv request vote from 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 1 > recv request vote result from 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n");

    CLUSTER_STATE(1, RAFT_LEADER);

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
        "[   0] 1 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 2 > start - T:1 V:0 S:0/0 L:1/1\n");

    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1149] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1164] 2 > recv request vote from 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* Disconnect the second server, so the first server does not receive the
     * result and eventually starts a new election round. */
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[3008] 1 > tick\n"
        "           stay candidate and start new election for term 3\n");

    /* Reconnecting the two servers eventually makes the first server win the
     * election. */
    CLUSTER_RECONNECT(2, 1);

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[3023] 2 > recv request vote from 1\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[3038] 1 > recv request vote result from 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n");

    CLUSTER_STATE(1, RAFT_LEADER);

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
        "[   0] 1 > start - T:2 V:0 S:0/0 L:1/2\n"
        "[   0] 2 > start - T:1 V:0 S:0/0 L:1/2\n");

    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1149] 1 > tick\n"
        "           convert to candidate and start new election for term 3\n");

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1164] 2 > recv request vote from 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log equal or longer (2/1 vs 2/1) -> grant vote\n");

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 1 > recv request vote result from 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           send 0 entries starting at 2 to server 2 (last index 2)\n");

    CLUSTER_STATE(1, RAFT_LEADER);

    return MUNIT_OK;
}

/* If the requester last log entry index is higher, the vote is granted. */
TEST(election, grantIfLastIndexIsHigher, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(1 /* ID */, 2 /* N servers */, 2 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    CLUSTER_PERSIST_COMMAND(1 /*ID */, 1 /* term */, 1 /* payload */);
    CLUSTER_PERSIST_TERM(1 /* ID */, 2 /* term */);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > start - T:2 V:0 S:0/0 L:1/2\n"
        "[   0] 2 > start - T:1 V:0 S:0/0 L:1/1\n");

    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1149] 1 > tick\n"
        "           convert to candidate and start new election for term 3\n");

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1164] 2 > recv request vote from 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log equal or longer (2/1 vs 1/1) -> grant "
        "vote\n");

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 1 > recv request vote result from 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           send 0 entries starting at 2 to server 2 (last index 2)\n");

    CLUSTER_STATE(1, RAFT_LEADER);

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
        "[   0] 1 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 2 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 3 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 4 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 5 > start - T:1 V:0 S:0/0 L:1/1\n");

    /* The first server converts to candidate and sends vote requests. */
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1149] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;

    /* All servers receive the request, grant their vote and send the reply. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1164] 2 > recv request vote from 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[1164] 3 > recv request vote from 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[1164] 4 > recv request vote from 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[1164] 5 > recv request vote from 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* The first server receives the first RequestVote result RPC but stays
     * candidate since it has only 2 votes, and 3 are required. */
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 1 > recv request vote result from 2\n"
        "           votes quorum not reached\n");

    /* The first server receives the second RequestVote result RPC and converst
     * to leader. */
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 1 > recv request vote result from 3\n"
        "           votes quorum reached -> convert to leader\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "           send 0 entries starting at 1 to server 4 (last index 1)\n"
        "           send 0 entries starting at 1 to server 5 (last index 1)\n");

    CLUSTER_STATE(1, RAFT_LEADER);

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
        "[   0] 1 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 2 > start - T:3 V:0 S:0/0 L:1/1\n");

    CLUSTER_STEP;
    CLUSTER_STATE(1, RAFT_CANDIDATE);
    CLUSTER_TRACE(
        "[1149] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1164] 2 > recv request vote from 1\n"
        "           remote term is lower (2 vs 3) -> reject\n");

    /* The first server receives the RequestVote result RPC and converts to
     * follower because it discovers the newer term. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 1 > recv request vote result from 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n");

    CLUSTER_STATE(1, RAFT_FOLLOWER);

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

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 2 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 3 > start - T:1 V:0 S:0/0 L:1/1\n");

    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1149] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* Server 1 wins the elections. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1164] 2 > recv request vote from 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n"
        "[1164] 3 > recv request vote from 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant "
        "vote\n");

    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 1 > recv request vote result from 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n");

    CLUSTER_STATE(1, RAFT_LEADER);

    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 1 > recv request vote result from 3\n"
        "           local server is leader -> ignore\n");

    /* Server 2 gets disconnected and becomes candidate. */
    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1194] 3 > recv append entries from 1\n"
        "[1209] 1 > recv append entries result from 3\n"
        "[1279] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[1294] 3 > recv append entries from 1\n"
        "[1309] 1 > recv append entries result from 3\n"
        "[1379] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[1394] 3 > recv append entries from 1\n"
        "[1409] 1 > recv append entries result from 3\n"
        "[1479] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[1494] 3 > recv append entries from 1\n"
        "[1509] 1 > recv append entries result from 3\n"
        "[1579] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[1594] 3 > recv append entries from 1\n"
        "[1609] 1 > recv append entries result from 3\n"
        "[1679] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[1694] 3 > recv append entries from 1\n"
        "[1709] 1 > recv append entries result from 3\n"
        "[1779] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[1794] 3 > recv append entries from 1\n"
        "[1809] 1 > recv append entries result from 3\n"
        "[1879] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[1894] 3 > recv append entries from 1\n"
        "[1909] 1 > recv append entries result from 3\n"
        "[1979] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[1994] 3 > recv append entries from 1\n"
        "[2009] 1 > recv append entries result from 3\n"
        "[2079] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[2094] 3 > recv append entries from 1\n"
        "[2109] 1 > recv append entries result from 3\n"
        "[2179] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[2194] 3 > recv append entries from 1\n"
        "[2209] 1 > recv append entries result from 3\n"
        "[2279] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[2294] 3 > recv append entries from 1\n"
        "[2309] 1 > recv append entries result from 3\n"
        "[2379] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n"
        "[2394] 3 > recv append entries from 1\n"
        "[2409] 1 > recv append entries result from 3\n");
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[2462] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n");

    CLUSTER_STATE(2, RAFT_CANDIDATE);

    /* Server 2 stays candidate since its requests get rejected. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[2477] 1 > recv request vote from 2\n"
        "           local server has a leader -> reject\n");
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[2477] 3 > recv request vote from 2\n"
        "           local server has a leader -> reject\n");
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[2479] 1 > tick\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n");
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[2492] 2 > recv request vote result from 3\n"
        "           remote term is lower (2 vs 3) -> ignore\n");

    CLUSTER_STATE(2, RAFT_CANDIDATE);

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
    raft_seed(CLUSTER_GET(2), 1);

    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 2 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 3 > start - T:1 V:0 S:0/0 L:1/1\n");

    /* Server 1 and server 2 both become candidates. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1149] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1149] 2 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* Server 3 receives the vote request from server 1 and grants it. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1164] 3 > recv request vote from 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log equal or longer (1/1 vs 1/1) -> grant vote\n");

    /* Server 2 receives the vote request from server 1 and rejects it because
     * it has already voted. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1164] 3 > recv request vote from 2\n"
        "           local server already voted for 1 -> don't grant vote\n");

    /* Server 0 receives the vote result from server 2 and becomes leader. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 1 > recv request vote result from 3\n"
        "           votes quorum reached -> convert to leader\n"
        "           send 0 entries starting at 1 to server 2 (last index 1)\n"
        "           send 0 entries starting at 1 to server 3 (last index 1)\n");

    /* Server 1 is still candidate because its vote request got rejected. */
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 2 > recv request vote result from 3\n"
        "           vote not granted\n");

    CLUSTER_STATE(2, RAFT_CANDIDATE);

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
        "[   0] 1 > start - T:1 V:0 S:0/0 L:1/2\n"
        "[   0] 2 > start - T:1 V:0 S:0/0 L:1/2\n");

    /* The first server becomes candidate. */
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1149] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1164] 2 > recv request vote from 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log has lower term (2/1 vs 2/2) -> don't grant "
        "vote\n");

    /* The first server receives the response and stays candidate. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 1 > recv request vote result from 2\n"
        "           vote not granted\n");

    CLUSTER_STATE(1, RAFT_CANDIDATE);

    /* Eventually the second server becomes leader because it has a longer
     * log. */
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1298] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n");
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1313] 1 > recv request vote from 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           remote log has higher term (2/2 vs 2/1) -> grant vote\n");
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1328] 2 > recv request vote result from 1\n"
        "           votes quorum reached -> convert to leader\n"
        "           send 0 entries starting at 2 to server 1 (last index 2)\n");

    CLUSTER_STATE(2, RAFT_LEADER);

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
        "[   0] 1 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 2 > start - T:1 V:0 S:0/0 L:1/2\n");

    /* The first server becomes candidate. */
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1149] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1164] 2 > recv request vote from 1\n"
        "           remote term is higher (2 vs 1) -> bump term\n"
        "           remote log shorter (1/1 vs 2/1) -> don't grant vote\n");

    /* The first server receives the response and stays candidate. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1179] 1 > recv request vote result from 2\n"
        "           vote not granted\n");

    CLUSTER_STATE(1, RAFT_CANDIDATE);

    /* Eventually the second server becomes leader because it has a longer
     * log. */
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1298] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n");
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1313] 1 > recv request vote from 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           remote log equal or longer (2/1 vs 1/1) -> grant vote\n");
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1328] 2 > recv request vote result from 1\n"
        "           votes quorum reached -> convert to leader\n"
        "           send 0 entries starting at 2 to server 1 (last index 2)\n");

    CLUSTER_STATE(2, RAFT_LEADER);

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
        "[   0] 1 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 2 > start - T:1 V:0 S:0/0 L:1/1\n"
        "[   0] 3 > start - T:1 V:0 S:0/0 L:1/1\n");

    /* Server 1 becomes candidate. */
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1149] 1 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    /* Server 1 stays candidate because it can't reach a quorum and eventually
     * server 2 becomes candidate as well. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_STEP;
    CLUSTER_TRACE(
        "[1298] 2 > tick\n"
        "           convert to candidate and start new election for term 2\n");

    CLUSTER_STATE(1, RAFT_CANDIDATE);
    CLUSTER_STATE(2, RAFT_CANDIDATE);

    return MUNIT_OK;
}

static char *ioErrorConvertDelay[] = {"0", "1", NULL};
static MunitParameterEnum ioErrorConvert[] = {
    {"delay", ioErrorConvertDelay},
    {NULL, NULL},
};

/* An I/O error occurs when converting to candidate. */
TEST(election, ioErrorConvert, setUp, tearDown, 0, ioErrorConvert)
{
    /*
    struct fixture *f = data;
    const char *delay = munit_parameters_get(params, "delay");
    return MUNIT_SKIP;
    CLUSTER_START;

    */
    /* The first server fails to convert to candidate. */
    /*
      CLUSTER_IO_FAULT(0, atoi(delay), 1);
    CLUSTER_STEP;
    ASSERT_UNAVAILABLE(0);
    */

    return MUNIT_OK;
}

/* The I/O error occurs when sending a vote request, and gets ignored. */
TEST(election, ioErrorSendVoteRequest, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    return MUNIT_SKIP;
    CLUSTER_START;

    */
    /* The first server fails to send a RequestVote RPC. */
    /*
    CLUSTER_IO_FAULT(0, 2, 1);
    CLUSTER_STEP;
    */
    /* The first server is still candidate. */
    /*
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);
    */

    return MUNIT_OK;
}

/* The I/O error occurs when the second node tries to persist its vote. */
TEST(election, ioErrorPersistVote, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    return MUNIT_SKIP;
    CLUSTER_START;

    */
    /* The first server becomes candidate. */
    /*
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    */
    /* The second server receives a RequestVote RPC but fails to persist its
     * vote. */
    /*
    CLUSTER_IO_FAULT(1, 0, 1);
    CLUSTER_STEP;
    ASSERT_UNAVAILABLE(1);
    */

    return MUNIT_OK;
}

/* Test an election round with two voters and pre-vote. */
TEST(election, preVote, setUp, tearDown, 0, NULL)
{
    /*
    struct fixture *f = data;
    raft_set_pre_vote(CLUSTER_RAFT(0), true);
    raft_set_pre_vote(CLUSTER_RAFT(1), true);
    CLUSTER_START;

    */
    /* The first server eventually times out and converts to candidate, but it
     * does not increment its term yet.*/
    /*
    STEP_UNTIL_CANDIDATE(0);
    ASSERT_TIME(1000);
    ASSERT_TERM(0, 1);

    */
    // CLUSTER_STEP; /* Server 1 tick */
    /*
    ASSERT_FOLLOWER(1);
    */

    // CLUSTER_STEP; /* Server 0 completes sending a pre-vote RequestVote RPC */
    // CLUSTER_STEP; /* Server 1 receives the pre-vote RequestVote RPC */
    // ASSERT_TERM(1, 1); /* Server 1 does increment its term */
    // ASSERT_VOTED_FOR(1, 0); /* Server 1 does not persist its vote */
    /*
    ASSERT_TIME(1015);
    */

    // CLUSTER_STEP; /* Server 1 completes sending pre-vote RequestVote result */
    // CLUSTER_STEP; /* Server 0 receives the pre-vote RequestVote result */
    // ASSERT_CANDIDATE(0);
    // ASSERT_TERM(0, 2); /* Server 0 has now incremented its term. */
    /*
    ASSERT_TIME(1030);
    */

    // CLUSTER_STEP; /* Server 1 completes sending an actual RequestVote RPC */
    // CLUSTER_STEP; /* Server 1 receives the actual RequestVote RPC */
    // ASSERT_TERM(1, 2); /* Server 1 does increment its term. */
    // ASSERT_VOTED_FOR(1, 1); /* Server 1 does persists its vote */

    // CLUSTER_STEP; /* Server 1 completes sending actual RequestVote result */
    // CLUSTER_STEP; /* Server 0 receives the actual RequestVote result */
    /*
    ASSERT_LEADER(0);
    */

    return MUNIT_OK;
}

/* A candidate receives votes then crashes. */
TEST(election, preVoteWithcandidateCrash, setUp, tearDown, 0, NULL /* cluster_3_params */)
{
    /*
    struct fixture *f = data;
    raft_set_pre_vote(CLUSTER_RAFT(0), true);
    raft_set_pre_vote(CLUSTER_RAFT(1), true);
    raft_set_pre_vote(CLUSTER_RAFT(2), true);
    CLUSTER_START;

    */
    /* The first server eventually times out and converts to candidate, but it
     * does not increment its term yet.*/
    /*
    STEP_UNTIL_CANDIDATE(0);
    ASSERT_TIME(1000);
    ASSERT_TERM(0, 1);

    */
     /* Server 1 and 2 ticks */
    /*
    CLUSTER_STEP_N(2);
    ASSERT_FOLLOWER(1);
    ASSERT_FOLLOWER(2);

    */
    /* Server 0 completes sending a pre-vote RequestVote RPCs */
    /*
    CLUSTER_STEP_N(2);
    */

    // CLUSTER_STEP; /* Server 1 receives the pre-vote RequestVote RPC */
    // ASSERT_TERM(1, 1); /* Server 1 does not increment its term */
    // ASSERT_VOTED_FOR(1, 0); /* Server 1 does not persist its vote */
    /*
    ASSERT_TIME(1015);
    */

    // CLUSTER_STEP; /* Server 2 receives the pre-vote RequestVote RPC */
    // ASSERT_TERM(2, 1); /* Server 2 does not increment its term */
    // ASSERT_VOTED_FOR(2, 0); /* Server 1 does not persist its vote */
    /*
    ASSERT_TIME(1015);
    */

    /* Server 1 and 2 complete sending pre-vote RequestVote results */
    /*
    CLUSTER_STEP_N(2);

    */
    /* Server 0 receives the pre-vote RequestVote results */
    /*
    CLUSTER_STEP_N(2);
    ASSERT_CANDIDATE(0);
    */
    // ASSERT_TERM(0, 2); /* Server 0 has now incremented its term. */
    /*
    ASSERT_TIME(1030);

    */
    /* Server 1 completes sending actual RequestVote RPCs */
    /*
    CLUSTER_STEP_N(2);

    */
    // CLUSTER_STEP; /* Server 1 receives the actual RequestVote RPC */
    // ASSERT_TERM(1, 2); /* Server 1 does increment its term. */
    // ASSERT_VOTED_FOR(1, 1); /* Server 1 does persists its vote */

    // CLUSTER_STEP; /* Server 2 receives the actual RequestVote RPC */
    // ASSERT_TERM(2, 2); /* Server 2 does increment its term. */
    // ASSERT_VOTED_FOR(2, 1); /* Server 2 does persists its vote */

    /* Server 0 crashes. */
    /*
    CLUSTER_KILL(0);

    */
    /* Server 1 times out and starts an election. It doesn't increment its term
     * yet but it reset its vote since it's beginning the pre-vote phase. */
    /*
    STEP_UNTIL_CANDIDATE(1);
    ASSERT_TIME(2200);
    ASSERT_TERM(1, 2);
    ASSERT_VOTED_FOR(1, 0);

    */
    /* Since server 2 has already voted for server 0, it doesn't grant its vote
     * and eventually times out and becomes candidate, resetting its vote as
     * well. */
    /*
    STEP_UNTIL_CANDIDATE(2);
    ASSERT_TIME(2300);
    ASSERT_TERM(2, 2);
    ASSERT_VOTED_FOR(2, 0);

    */
    /* Server 2 completes sending the pre-vote RequestVote RPCs */
    /*
    CLUSTER_STEP_N(2);

    */
    // CLUSTER_STEP; /* Server 1 receives the pre-vote RequestVote RPC */
    // ASSERT_TERM(1, 2); /* Server 1 does not increment its term */
    // ASSERT_VOTED_FOR(1, 0); /* Server 1 does not persist its vote */

    /* Server 1 completes sending pre-vote RequestVote results */
    /*
    CLUSTER_STEP_N(2);

    */
    /* Server 2 receives the pre-vote RequestVote results */
    /*
    CLUSTER_STEP;
    ASSERT_CANDIDATE(2);
    */
    // ASSERT_TERM(2, 3); /* Server 2 has now incremented its term. */
    /*
    ASSERT_TIME(2330);

    */
    /* Server 2 completes sending actual RequestVote RPCs */
    /*
    CLUSTER_STEP_N(2);

    */
    // CLUSTER_STEP_N(2); /* Server 1 receives the actual RequestVote RPC */
    // ASSERT_TERM(1, 3); /* Server 1 does increment its term. */
    // ASSERT_VOTED_FOR(1, 3); /* Server 1 does persists its vote */

    // CLUSTER_STEP; /* Server 1 completes sending actual RequestVote result */
    // CLUSTER_STEP; /* Server 2 receives the actual RequestVote result */
    /*
    ASSERT_LEADER(2);
    */

    return MUNIT_OK;
}
