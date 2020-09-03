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
        "           probe server 3 sending 1 entries with first index 2\n"
        "[ 120] 1 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n");

    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

SUITE(raft_transfer)

/* The follower we ask to transfer leadership to is up-to-date. */
TEST(raft_transfer, upToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int rv;

    /* Wait for the initial no-op entry to be replicated by server 2. */
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 140] 3 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n");

    /* Transfer leadership to server 2. */
    rv = raft_transfer(CLUSTER_GET(1), 2);
    munit_assert_int(rv, ==, 0);

    CLUSTER_TRACE(
        "[ 150] 1 > transfer leadership to 2\n"
        "           server 2 is up-to-date > send timeout now message\n"
        "[ 150] 1 > recv append entries result from server 3\n"
        "           replicated index 2 lower or equal than commit index 2\n"
        "[ 160] 2 > recv timeout now from server 1\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 160] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 3\n"
        "           pipeline server 3 sending 0 entries with first index 3\n"
        "[ 170] 1 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           remote log equal or longer (2/2 vs 2/2) -> grant vote\n"
        "[ 170] 3 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log equal or longer (2/2 vs 2/2) -> grant vote\n"
        "[ 170] 2 > recv append entries from server 1\n"
        "           local term is higher -> reject \n"
        "[ 170] 3 > recv append entries from server 1\n"
        "           local term is higher -> reject \n"
        "[ 180] 2 > recv request vote result from server 1\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 1 sending 1 entries with first index 3\n"
        "           probe server 3 sending 1 entries with first index 3\n");

    return MUNIT_OK;
}

/* The follower we ask to transfer leadership to needs to catch up. */
TEST(raft_transfer, catchUp, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int rv;

    /* Transfer leadership to server 2, which will first need to catch-up with
     * the no-op entry that we just replicated. */
    rv = raft_transfer(CLUSTER_GET(1), 2);
    munit_assert_int(rv, ==, 0);

    /* Eventually server 2 catches up, and server 1 sends it a TimeoutNow
     * message. */
    CLUSTER_TRACE(
        "[ 120] 1 > transfer leadership to 2\n"
        "           server 2 not up-to-date > wait for it to catch-up\n"
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 140] 3 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n"
        "           server 2 caught up > send timeout now message\n"
        "[ 150] 1 > recv append entries result from server 3\n"
        "           replicated index 2 lower or equal than commit index 2\n");

    /* Server 2 starts an election and wins it. */
    CLUSTER_TRACE(
        "[ 160] 2 > recv timeout now from server 1\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 160] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 3\n"
        "           pipeline server 3 sending 0 entries with first index 3\n"
        "[ 170] 1 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term, step down\n"
        "           remote log equal or longer (2/2 vs 2/2) -> grant vote\n"
        "[ 170] 3 > recv request vote from server 2\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log equal or longer (2/2 vs 2/2) -> grant vote\n"
        "[ 170] 2 > recv append entries from server 1\n"
        "           local term is higher -> reject \n"
        "[ 170] 3 > recv append entries from server 1\n"
        "           local term is higher -> reject \n"
        "[ 180] 2 > recv request vote result from server 1\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 1 sending 1 entries with first index 3\n"
        "           probe server 3 sending 1 entries with first index 3\n");

    return MUNIT_OK;
}

/* The follower we ask to transfer leadership to is down and the leadership
 * transfer does not succeed. */
TEST(raft_transfer, expire, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_GET(1);
    int rv;

    CLUSTER_KILL(2);
    rv = raft_transfer(raft, 2);
    munit_assert_int(rv, ==, 0);

    /* Lower server's 1 election timeout to expire the leadership transfer
     * earlier. */
    raft_set_election_timeout(raft, 40);

    munit_assert_int(raft->transfer.id, ==, 2);

    CLUSTER_TRACE(
        "[ 120] 1 > transfer leadership to 2\n"
        "           server 2 not up-to-date > wait for it to catch-up\n"
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 3 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 3\n"
        "           replication quorum reached -> commit index 2\n"
        "[ 160] 1 > tick\n"
        "           probe server 2 sending 1 entries with first index 2\n"
        "           pipeline server 3 sending 0 entries with first index 3\n"
        "           transfer to 2 pending since 40 msecs -> expire\n");

    return MUNIT_OK;
}

/* The given ID doesn't match any server in the current configuration. */
TEST(raft_transfer, unknownServer, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int rv;
    rv = raft_transfer(CLUSTER_GET(1), 4);
    munit_assert_int(rv, ==, RAFT_BADID);
    return MUNIT_OK;
}

/* Submitting a transfer request twice is an error. */
TEST(raft_transfer, twice, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int rv;
    rv = raft_transfer(CLUSTER_GET(1), 2);
    munit_assert_int(rv, ==, 0);
    rv = raft_transfer(CLUSTER_GET(1), 3);
    munit_assert_int(rv, ==, RAFT_NOTLEADER);
    return MUNIT_OK;
}

/* If the given ID is zero, the target is selected automatically. */
TEST(raft_transfer, autoSelect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_GET(1);
    int rv;
    rv = raft_transfer(raft, 0);
    munit_assert_int(rv, ==, 0);
    munit_assert_int(raft->transfer.id, ==, 3);
    return MUNIT_OK;
}

/* If the given ID is zero, the target is selected automatically. Followers that
 * are up-to-date are preferred. */
TEST(raft_transfer, autoSelectUpToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_GET(1);
    int rv;
    CLUSTER_KILL(2);
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 3 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 3\n"
        "           replication quorum reached -> commit index 2\n");

    rv = raft_transfer(raft, 0);
    munit_assert_int(rv, ==, 0);
    munit_assert_int(raft->transfer.id, ==, 3);

    CLUSTER_TRACE(
        "[ 150] 1 > transfer leadership to 0\n"
        "           server 3 is up-to-date > send timeout now message\n");

    return MUNIT_OK;
}

/* It's possible to transfer leadership also after the server has been
 * demoted. */
TEST(raft_transfer, afterDemotion, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    struct raft_entry entry;
    struct raft_configuration conf;
    int rv;
    raft_configuration_init(&conf);
    rv = raft_configuration_add(&conf, 1, "1", RAFT_SPARE);
    munit_assert_int(rv, ==, 0);
    rv = raft_configuration_add(&conf, 2, "2", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);
    rv = raft_configuration_add(&conf, 3, "3", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);

    entry.type = RAFT_CHANGE;
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&conf);

    raft = CLUSTER_GET(1);
    rv = raft_accept(raft, &entry, 1);
    munit_assert_int(rv, ==, 0);

    CLUSTER_TRACE(
        "[ 120] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 2 sending 2 entries with first index 2\n"
        "           probe server 3 sending 2 entries with first index 2\n"
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 140] 3 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 140] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 140] 3 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum not reached for index 2\n"
        "           pipeline server 2 sending 1 entries with first index 3\n"
        "[ 150] 1 > recv append entries result from server 3\n"
        "           replication quorum reached -> commit index 2\n"
        "           pipeline server 3 sending 1 entries with first index 3\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum not reached for index 3\n"
        "[ 150] 1 > recv append entries result from server 3\n"
        "           replication quorum reached -> commit index 3\n");

    munit_assert_int(raft->configuration_index, ==, 3);
    munit_assert_int(raft->configuration.servers[0].role, ==, RAFT_SPARE);

    rv = raft_transfer(raft, 3);
    munit_assert_int(rv, ==, 0);

    CLUSTER_TRACE(
        "[ 150] 1 > transfer leadership to 3\n"
        "           server 3 is up-to-date > send timeout now message\n"
        "[ 160] 2 > recv append entries from server 1\n"
        "[ 160] 3 > recv append entries from server 1\n"
        "[ 160] 3 > recv timeout now from server 1\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 160] 1 > tick\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "[ 170] 1 > recv append entries result from server 3\n"
        "[ 170] 2 > recv request vote from server 3\n"
        "           remote term is higher (3 vs 2) -> bump term\n"
        "           remote log equal or longer (3/2 vs 3/2) -> grant vote\n"
        "[ 180] 3 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 4\n"
        "           probe server 2 sending 1 entries with first index 4\n");

    return MUNIT_OK;
}
