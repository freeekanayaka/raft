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

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Set the snapshot threshold on all servers of the cluster */
#define SET_SNAPSHOT_THRESHOLD(VALUE)                            \
    {                                                            \
        unsigned i;                                              \
        for (i = 0; i < CLUSTER_N; i++) {                        \
            raft_set_snapshot_threshold(CLUSTER_RAFT(i), VALUE); \
        }                                                        \
    }

/* Set the snapshot trailing logs number on all servers of the cluster */
#define SET_SNAPSHOT_TRAILING(VALUE)                            \
    {                                                           \
        unsigned i;                                             \
        for (i = 0; i < CLUSTER_N; i++) {                       \
            raft_set_snapshot_trailing(CLUSTER_RAFT(i), VALUE); \
        }                                                       \
    }

/******************************************************************************
 *
 * Successfully install a snapshot
 *
 *****************************************************************************/

SUITE(snapshot)

/* Install a snapshot on a follower that has fallen behind. */
TEST(snapshot, installOne, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    raft_id id;
    struct raft_entry entry;
    unsigned i;
    int rv;

    /* Set very low value for snapshot trailing entries number */
    for (id = 1; id <= 3; id++) {
        raft_set_snapshot_trailing(CLUSTER_GET(id), 1);
    }

    /* Disconnect server 1 from server 3, so server 3 falls behind. */
    CLUSTER_DISCONNECT(1, 3);

    /* Apply a few of entries, then take a snapshot. */
    for (i = 0; i < 2; i++) {
        entry.type = RAFT_COMMAND;
        entry.buf.len = 8;
        entry.buf.base = raft_malloc(entry.buf.len);
        rv = raft_accept(CLUSTER_GET(1), &entry, 1);
        munit_assert_int(rv, ==, 0);
    }

    CLUSTER_TRACE(
        "[ 120] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "           probe server 2 sending 2 entries with first index 2\n"
        "           probe server 3 sending 2 entries with first index 2\n"
        "[ 120] 1 > accept 1 commands starting at 4\n"
        "           persist 1 entries with first index 4\n"
        "           probe server 2 sending 3 entries with first index 2\n"
        "           probe server 3 sending 3 entries with first index 2\n"
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n"
        "[ 130] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 130] 1 > done persist 1 entries with first index 4 (status 0)\n"
        "           replication quorum not reached for index 4\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 4\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 140] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 140] 2 > done persist 1 entries with first index 4 (status 0)\n"
        "           replicated index 4 -> send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n"
        "           pipeline server 2 sending 2 entries with first index 3\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 3\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 4\n");

    CLUSTER_PERSIST_SNAPSHOT(1, /* id                                       */
                             4, /* index                                    */
                             2, /* term                                     */
                             3, /* n                                        */
                             3, /* n voting                                 */
                             1 /*  conf index                               */);
    rv = raft_snapshot(CLUSTER_GET(1), 4 /* index */);
    munit_assert_int(rv, ==, 0);

    /* Reconnect the follower and wait for it to catch up */
    CLUSTER_RECONNECT(1, 3);

    CLUSTER_TRACE(
        "[ 150] 1 > take snapshot with last index 4, leave 1 trailing entries\n"
        "[ 160] 2 > recv append entries from server 1\n"
        "[ 160] 1 > tick\n"
        "           catch-up server 3 sending snapshot for missing index 1\n"
        "[ 170] 1 > recv append entries result from server 2\n"
        "[ 170] 3 > recv install snapshot from server 1\n"
        "           persist snapshot with last index 4 at term 4\n"
        "[ 180] 3 > done persist snapshot with last index 4 (status 0)\n"
        "           restored snapshot with last index 4\n"
        "           replicated index 4 -> send success result to 1\n"
        "[ 190] 1 > recv append entries result from server 3\n"
        "           replicated index 4 lower or equal than commit index 4\n"
        "[ 200] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 5\n"
        "           probe server 3 sending 0 entries with first index 5\n"
        "[ 210] 2 > recv append entries from server 1\n"
        "[ 210] 3 > recv append entries from server 1\n"
        "[ 220] 1 > recv append entries result from server 2\n"
        "[ 220] 1 > recv append entries result from server 3\n"
        "[ 240] 1 > tick\n"
        "           pipeline server 2 sending 0 entries with first index 5\n"
        "           pipeline server 3 sending 0 entries with first index 5\n");

    return MUNIT_OK;
}
