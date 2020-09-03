#include "../lib/cluster.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

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

SUITE(raft_accept)

/* If the raft instance is not in leader state, an error is returned. */
TEST(raft_accept, notLeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    int rv;

    entry.type = RAFT_COMMAND;
    entry.buf.len = 8;
    entry.buf.base = raft_malloc(entry.buf.len);

    rv = raft_accept(CLUSTER_GET(2), &entry, 1);
    munit_assert_int(rv, ==, RAFT_NOTLEADER);

    raft_free(entry.buf.base);

    return MUNIT_OK;
}

/* The raft instance steps down from leader state, the entry is not committed
 * and gets rolled back by the next leader. */
TEST(raft_accept, rollbackAfterLeadershipLost, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    int rv;

    /* Server 1 successfully replicates the first no-op entry. */
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

    /* Server 1 accepts a new entry but disconnects right after, eventually
     * stepping down. */
    entry.type = RAFT_COMMAND;
    entry.buf.len = 8;
    entry.buf.base = raft_malloc(entry.buf.len);

    rv = raft_accept(CLUSTER_GET(1), &entry, 1);
    munit_assert_int(rv, ==, 0);

    raft_set_election_timeout(CLUSTER_GET(1), 30);

    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 1);

    CLUSTER_TRACE(
        "[ 150] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "           pipeline server 2 sending 1 entries with first index 3\n"
        "[ 160] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 160] 1 > tick\n"
        "           probe server 2 sending 1 entries with first index 3\n"
        "           probe server 3 sending 2 entries with first index 2\n"
        "[ 200] 1 > tick\n"
        "           unable to contact majority of cluster -> step down\n");

    raft_set_election_timeout(CLUSTER_GET(1), 100);

    /* Server 2 and 3 both timeout and run a new election, which server 2
     * wins. */
    raft_seed(CLUSTER_GET(2), 10);
    raft_set_election_timeout(CLUSTER_GET(2), 60);
    CLUSTER_TRACE(
        "[ 220] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 230] 3 > recv request vote from server 2\n"
        "           local server has a leader -> reject\n"
        "[ 240] 2 > recv request vote result from server 3\n"
        "           remote term is lower (2 vs 3) -> ignore\n"
        "[ 290] 3 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 299] 2 > tick\n"
        "           stay candidate and start new election for term 4\n"
        "[ 300] 2 > recv request vote from server 3\n"
        "           remote term is lower (3 vs 4) -> reject\n"
        "[ 305] 1 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 309] 3 > recv request vote from server 2\n"
        "           remote term is higher (4 vs 3) -> bump term, step down\n"
        "           remote log equal or longer (2/2 vs 2/2) -> grant vote\n"
        "[ 310] 3 > recv request vote result from server 2\n"
        "           local server is follower -> ignore\n"
        "[ 319] 2 > recv request vote result from server 3\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 1 sending 1 entries with first index 3\n"
        "           probe server 3 sending 1 entries with first index 3\n");

    /* Server 1 reconnects and its uncommitted entry is rolled back. */
    CLUSTER_RECONNECT(1, 2);
    CLUSTER_RECONNECT(2, 1);

    CLUSTER_TRACE(
        "[ 329] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 329] 1 > recv append entries from server 2\n"
        "           remote term is higher (4 vs 3) -> bump term, step down\n"
        "           local log has index 3 at term 2, remote at 4 -> truncate\n"
        "           persist 1 entries with first index 3\n");

    return MUNIT_OK;
}

/* The raft instance steps down from leader state, the entry is persisted on a
 * quorum of nodes and the new leader commits it. */
TEST(raft_accept, commitAfterLeadershipLost, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    int rv;

    /* Server 1 successfully replicates the first no-op entry. */
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

    /* Server 1 accepts a new entry and disconnects right after successfully
     * sending replication messages. I eventually steps down. */
    entry.type = RAFT_COMMAND;
    entry.buf.len = 8;
    entry.buf.base = raft_malloc(entry.buf.len);

    rv = raft_accept(CLUSTER_GET(1), &entry, 1);
    munit_assert_int(rv, ==, 0);

    raft_set_election_timeout(CLUSTER_GET(1), 30);

    CLUSTER_TRACE(
        "[ 150] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "           pipeline server 2 sending 1 entries with first index 3\n"
        "[ 150] 1 > recv append entries result from server 3\n"
        "           replicated index 2 lower or equal than commit index 2\n"
        "           pipeline server 3 sending 1 entries with first index 3\n"
        "[ 160] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 160] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 160] 3 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n");

    CLUSTER_DISCONNECT(1, 2);
    CLUSTER_DISCONNECT(2, 1);
    CLUSTER_DISCONNECT(1, 3);
    CLUSTER_DISCONNECT(3, 1);

    raft_set_election_timeout(CLUSTER_GET(1), 60);

    /* Server 2 and 3 both timeout and run a new election, which server 2
     * wins. */
    raft_seed(CLUSTER_GET(2), 10);
    raft_set_election_timeout(CLUSTER_GET(2), 60);

    CLUSTER_TRACE(
        "[ 160] 1 > tick\n"
        "[ 170] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 170] 3 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 200] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 4\n"
        "           pipeline server 3 sending 0 entries with first index 4\n"
        "[ 240] 1 > tick\n"
        "           probe server 2 sending 1 entries with first index 3\n"
        "           probe server 3 sending 1 entries with first index 3\n"
        "[ 250] 2 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 260] 3 > recv request vote from server 2\n"
        "           local server has a leader -> reject\n"
        "[ 270] 2 > recv request vote result from server 3\n"
        "           remote term is lower (2 vs 3) -> ignore\n"
        "[ 280] 1 > tick\n"
        "           unable to contact majority of cluster -> step down\n"
        "[ 320] 3 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 329] 2 > tick\n"
        "           stay candidate and start new election for term 4\n"
        "[ 330] 2 > recv request vote from server 3\n"
        "           remote term is lower (3 vs 4) -> reject\n"
        "[ 339] 3 > recv request vote from server 2\n"
        "           remote term is higher (4 vs 3) -> bump term, step down\n"
        "           remote log equal or longer (3/2 vs 3/2) -> grant vote\n"
        "[ 340] 3 > recv request vote result from server 2\n"
        "           local server is follower -> ignore\n"
        "[ 349] 2 > recv request vote result from server 3\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 4\n"
        "           probe server 1 sending 1 entries with first index 4\n"
        "           probe server 3 sending 1 entries with first index 4\n");

    /* Eventually server 2 commits the entry that was initially accepted by
     * server 1. */
    CLUSTER_TRACE(
        "[ 359] 2 > done persist 1 entries with first index 4 (status 0)\n"
        "           replication quorum not reached for index 4\n"
        "[ 359] 3 > recv append entries from server 2\n"
        "           persist 1 entries with first index 4\n"
        "[ 369] 3 > done persist 1 entries with first index 4 (status 0)\n"
        "           replicated index 4 -> send success result to 2\n"
        "[ 377] 1 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 379] 2 > recv append entries result from server 3\n"
        "           replication quorum reached -> commit index 4\n");

    /* When server 1 reconnets, its entry does not get truncated. */
    CLUSTER_RECONNECT(1, 2);
    CLUSTER_RECONNECT(2, 1);
    CLUSTER_TRACE(
        "[ 389] 2 > tick\n"
        "           probe server 1 sending 4 entries with first index 1\n"
        "           pipeline server 3 sending 0 entries with first index 5\n"
        "[ 399] 1 > recv append entries from server 2\n"
        "           remote term is higher (4 vs 3) -> bump term, step down\n"
        "           persist 1 entries with first index 4\n");

    return 0;
}
