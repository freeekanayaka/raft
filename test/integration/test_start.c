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

SUITE(raft_start)

/* There are two servers. The first has a snapshot present and no other
 * entries. */
TEST(raft_start, oneSnapshotAndNoEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);

    CLUSTER_PERSIST_TERM(1 /* ID */, 2 /* term */);
    CLUSTER_PERSIST_SNAPSHOT(1, /* ID                                        */
                             6, /* last index                                */
                             2, /* last term                                 */
                             2, /* N servers                                 */
                             2, /* N voting                                  */
                             1 /* conf index                                */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 2, vote 0, snapshot 6/2, no entries\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log has higher term (6/2 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 7\n"
        "           probe server 2 sending 1 entries with first index 7\n");

    /* It then replicates the snapshot. */
    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 7 (status 0)\n"
        "           replication quorum not reached for index 7\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no entry at index 6 -> reject\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries to 2\n"
        "           catch-up server 2 sending snapshot for missing index 1\n"
        "[ 150] 2 > recv install snapshot from server 1\n"
        "           persist snapshot with last index 6 at term 6\n"
        "[ 160] 2 > done persist snapshot with last index 6 (status 0)\n"
        "           restored snapshot with last index 6\n"
        "           replicated index 6 -> send success result to 1\n"
        "[ 160] 1 > tick\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           replicated index 6 lower or equal than commit index 6\n"
        "[ 200] 1 > tick\n"
        "           probe server 2 sending 1 entries with first index 7\n"
        "[ 210] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 7\n"
        "[ 220] 2 > done persist 1 entries with first index 7 (status 0)\n"
        "           replicated index 7 -> send success result to 1\n");

    return MUNIT_OK;
}

/* There are two servers. The first has a snapshot along with some follow-up
 * entries. */
TEST(raft_start, oneSnapshotAndSomeFollowUpEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);

    CLUSTER_PERSIST_TERM(1 /* ID */, 2 /* term */);
    CLUSTER_PERSIST_SNAPSHOT(1, /* ID                                        */
                             6, /* last index                                */
                             2, /* last term                                 */
                             2, /* N servers                                 */
                             2, /* N voting                                  */
                             1 /* conf index                                */);
    CLUSTER_PERSIST_COMMAND(1, 2, 123);
    CLUSTER_PERSIST_COMMAND(1, 2, 456);

    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 2 /* N voting */);

    /* Server 1 becomes leader. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 2, vote 0, snapshot 6/2, entries 7/2 to 8/2\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 3\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (3 vs 1) -> bump term\n"
        "           remote log has higher term (8/2 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 9\n"
        "           probe server 2 sending 1 entries with first index 9\n");

    CLUSTER_TRACE(
        "[ 130] 1 > done persist 1 entries with first index 9 (status 0)\n"
        "           replication quorum not reached for index 9\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no entry at index 8 -> reject\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries to 2\n"
        "           catch-up server 2 sending snapshot for missing index 1\n"
        "[ 150] 2 > recv install snapshot from server 1\n"
        "           persist snapshot with last index 6 at term 6\n"
        "[ 160] 2 > done persist snapshot with last index 6 (status 0)\n"
        "           restored snapshot with last index 6\n"
        "           replicated index 6 -> send success result to 1\n"
        "[ 160] 1 > tick\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           replicated index 6 lower or equal than commit index 6\n"
        "[ 200] 1 > tick\n"
        "           probe server 2 sending 3 entries with first index 7\n"
        "[ 210] 2 > recv append entries from server 1\n"
        "           persist 3 entries with first index 7\n"
        "[ 220] 2 > done persist 3 entries with first index 7 (status 0)\n"
        "           replicated index 9 -> send success result to 1\n"
        "[ 230] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 9\n");

    return MUNIT_OK;
}

/* There are 3 servers, the first has an additional entry, the others don't
 * have. */
TEST(raft_start, twoEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    int rv;
    CLUSTER_GROW(3);
    CLUSTER_BOOTSTRAP(1 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(2 /* ID */, 3 /* N servers */, 3 /* N voting */);
    CLUSTER_BOOTSTRAP(3 /* ID */, 3 /* N servers */, 3 /* N voting */);

    CLUSTER_PERSIST_TERM(1, 3);
    CLUSTER_PERSIST_COMMAND(1, 3, 123);

    /* Server 1 becomes leader. */
    CLUSTER_START(1 /* ID */);
    CLUSTER_START(2 /* ID */);
    CLUSTER_START(3 /* ID */);
    CLUSTER_TRACE(
        "[   0] 1 > term 3, vote 0, no snapshot, entries 1/1 to 2/3\n"
        "[   0] 2 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[   0] 3 > term 1, vote 0, no snapshot, entries 1/1 to 1/1\n"
        "[ 100] 1 > tick\n"
        "           convert to candidate and start new election for term 4\n"
        "[ 110] 2 > recv request vote from server 1\n"
        "           remote term is higher (4 vs 1) -> bump term\n"
        "           remote log has higher term (2/3 vs 1/1) -> grant vote\n"
        "[ 110] 3 > recv request vote from server 1\n"
        "           remote term is higher (4 vs 1) -> bump term\n"
        "           remote log has higher term (2/3 vs 1/1) -> grant vote\n"
        "[ 120] 1 > recv request vote result from server 2\n"
        "           votes quorum reached -> convert to leader\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 2 sending 1 entries with first index 3\n"
        "           probe server 3 sending 1 entries with first index 3\n"
        "[ 120] 1 > recv request vote result from server 3\n"
        "           local server is leader -> ignore\n");

    CLUSTER_STEP;
    CLUSTER_STEP;

    /* Server 1 accepts a new entry and replicates the existing one. */
    entry.type = RAFT_COMMAND;
    entry.buf.base = raft_malloc(8);
    entry.buf.len = 8;
    rv = raft_accept(CLUSTER_GET(1), &entry, 1);
    munit_assert_int(rv, ==, 0);

    CLUSTER_TRACE(
        "[ 120] 1 > accept 1 commands starting at 4\n"
        "           persist 1 entries with first index 4\n"
        "[ 130] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           no entry at index 2 -> reject\n"
        "[ 130] 3 > recv append entries from server 1\n"
        "           no entry at index 2 -> reject\n"
        "[ 130] 1 > done persist 1 entries with first index 4 (status 0)\n"
        "           replication quorum not reached for index 4\n"
        "[ 140] 1 > recv append entries result from server 2\n"
        "           log mismatch -> send old entries to 2\n"
        "           probe server 2 sending 3 entries with first index 2\n"
        "[ 140] 1 > recv append entries result from server 3\n"
        "           log mismatch -> send old entries to 3\n"
        "           probe server 3 sending 3 entries with first index 2\n"
        "[ 150] 2 > recv append entries from server 1\n"
        "           persist 3 entries with first index 2\n"
        "[ 150] 3 > recv append entries from server 1\n"
        "           persist 3 entries with first index 2\n"
        "[ 160] 2 > done persist 3 entries with first index 2 (status 0)\n"
        "           replicated index 4 -> send success result to 1\n"
        "[ 160] 3 > done persist 3 entries with first index 2 (status 0)\n"
        "           replicated index 4 -> send success result to 1\n"
        "[ 160] 1 > tick\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 4\n");

    return MUNIT_OK;
}

/* There is a single voting server in the cluster, which immediately elects
 * itself when starting. */
TEST(raft_start, singleVotingSelfElect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(1);
    CLUSTER_BOOTSTRAP(1 /* ID */, 1 /* N servers */, 1 /* N voting */);
    CLUSTER_START(1 /* ID */);
    munit_assert_int(CLUSTER_GET(1)->state, ==, RAFT_LEADER);
    return MUNIT_OK;
}

/* There are two servers in the cluster, one is voting and the other is
 * not. When started, the non-voting server does not elects itself. */
TEST(raft_start, singleVotingNotUs, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW(2);
    CLUSTER_BOOTSTRAP(2 /* ID */, 2 /* N servers */, 1 /* N voting */);
    CLUSTER_START(2 /* ID */);
    munit_assert_int(CLUSTER_GET(2)->state, ==, RAFT_FOLLOWER);
    return MUNIT_OK;
}
