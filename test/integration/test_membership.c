#include "../../src/configuration.h"
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
    /*
    struct raft_change req;
    */
};

/* Set up a cluster of 2 servers, with the first as leader. */
static void *setup(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER;
    /*
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    CLUSTER_ELECT(0);
    */
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/* Add a an empty server to the cluster and start it. */
#define GROW                                \
    {                                       \
        int rv__;                           \
        CLUSTER_GROW;                       \
        rv__ = raft_start(CLUSTER_RAFT(2)); \
        munit_assert_int(rv__, ==, 0);      \
    }

/* Invoke raft_add against the I'th node and assert it returns the given
 * value. */
#define ADD(I, ID, RV)                                                \
    {                                                                 \
        int rv_;                                                      \
        char address_[16];                                            \
        sprintf(address_, "%d", ID);                                  \
        rv_ = raft_add(CLUSTER_RAFT(I), &f->req, ID, address_, NULL); \
        munit_assert_int(rv_, ==, RV);                                \
    }

/* Submit a request to assign the given ROLE to the server with the given ID. */
#define ASSIGN(I, ID, ROLE)                                          \
    {                                                                \
        int _rv;                                                     \
        _rv = raft_assign(CLUSTER_RAFT(I), &f->req, ID, ROLE, NULL); \
        munit_assert_int(_rv, ==, 0);                                \
    }

/* Invoke raft_remove against the I'th node and assert it returns the given
 * value. */
#define REMOVE(I, ID, RV)                                      \
    {                                                          \
        int rv_;                                               \
        rv_ = raft_remove(CLUSTER_RAFT(I), &f->req, ID, NULL); \
        munit_assert_int(rv_, ==, RV);                         \
    }

/* Assert the values of the committed and uncommitted configuration indexes on
 * the raft instance with the given index. */
#define ASSERT_CONFIGURATION_INDEXES(ID, COMMITTED, UNCOMMITTED)      \
    do {                                                              \
        struct raft *_raft_ = CLUSTER_GET(ID);                        \
        munit_assert_int(_raft_->configuration_index, ==, COMMITTED); \
        munit_assert_int(_raft_->configuration_uncommitted_index, ==, \
                         UNCOMMITTED);                                \
    } while (0)

/******************************************************************************
 *
 * raft_add
 *
 *****************************************************************************/

SUITE(raft_add)

/* After a request to add a new non-voting server is committed, the new
 * configuration is not marked as uncommitted anymore */
TEST(raft_add, committed, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_configuration conf;
    struct raft *raft;
    struct raft_server *server;
    int rv;
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
        "           probe server 2 sending 1 entries with first index 2\n"
        "[ 130] 1 > done persist 1 entries with first index 2 (status 0)\n"
        "           replication quorum not reached for index 2\n");

    raft_configuration_init(&conf);
    rv = raft_configuration_add(&conf, 1, "1", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);
    rv = raft_configuration_add(&conf, 2, "2", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);
    rv = raft_configuration_add(&conf, 3, "3", RAFT_SPARE);
    munit_assert_int(rv, ==, 0);

    entry.type = RAFT_CHANGE;
    rv = raft_configuration_encode(&conf, &entry.buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&conf);

    raft = CLUSTER_GET(1);
    rv = raft_accept(raft, &entry, 1);
    munit_assert_int(rv, ==, 0);

    /* The new configuration is already effective. */
    munit_assert_int(raft->configuration.n, ==, 3);
    server = &raft->configuration.servers[2];
    munit_assert_int(server->id, ==, 3);
    munit_assert_string_equal(server->address, "3");
    munit_assert_int(server->role, ==, RAFT_SPARE);

    /* The new configuration is marked as uncommitted. */
    ASSERT_CONFIGURATION_INDEXES(1, 1, 3);

    /* The next/match indexes now include an entry for the new server. */
    munit_assert_int(raft->leader_state.progress[2].next_index, ==, 4);
    munit_assert_int(raft->leader_state.progress[2].match_index, ==, 0);

    /* Replicate the configuration. */
    CLUSTER_TRACE(
        "[ 130] 1 > accept 1 commands starting at 3\n"
        "           persist 1 entries with first index 3\n"
        "[ 130] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 2\n"
        "[ 140] 1 > done persist 1 entries with first index 3 (status 0)\n"
        "           replication quorum not reached for index 3\n"
        "[ 140] 2 > done persist 1 entries with first index 2 (status 0)\n"
        "           replicated index 2 -> send success result to 1\n"
        "[ 150] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 2\n"
        "           pipeline server 2 sending 1 entries with first index 3\n"
        "[ 160] 2 > recv append entries from server 1\n"
        "           persist 1 entries with first index 3\n"
        "[ 160] 1 > tick\n"
        "[ 170] 2 > done persist 1 entries with first index 3 (status 0)\n"
        "           replicated index 3 -> send success result to 1\n"
        "[ 180] 1 > recv append entries result from server 2\n"
        "           replication quorum reached -> commit index 3\n");

    /* The new configuration is marked as committed. */
    ASSERT_CONFIGURATION_INDEXES(1, 3, 0);

    return MUNIT_OK;
}

/* Trying to add a server on a node which is not the leader results in an
 * error. */
TEST(raft_add, notLeader, setup, tear_down, 0, NULL)
{
    // struct fixture *f = data;
    // ADD(1 /*   I                                                     */,
    //    3 /*   ID                                                    */,
    //    RAFT_NOTLEADER);
    return MUNIT_OK;
}

/* Trying to add a server while a configuration change is already in progress
 * results in an error. */
TEST(raft_add, busy, setup, tear_down, 0, NULL)
{
    // struct fixture *f = data;
    //    ADD(0 /*   I                                                     */,
    //        3 /*   ID                                                    */,
    //        0);
    //    ADD(0 /*   I                                                     */,
    //        4 /*   ID                                                    */,
    //        RAFT_CANTCHANGE);
    return MUNIT_OK;
}

/* Trying to add a server with an ID which is already in use results in an
 * error. */
TEST(raft_add, duplicateId, setup, tear_down, 0, NULL)
{
    //    struct fixture *f = data;
    //    ADD(0 /*   I                                                     */,
    //        2 /*   ID                                                    */,
    //        RAFT_DUPLICATEID);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * raft_remove
 *
 *****************************************************************************/

SUITE(raft_remove)

/* After a request to remove server is committed, the new configuration is not
 * marked as uncommitted anymore */
TEST(raft_remove, committed, setup, tear_down, 0, NULL)
{
    /*
      struct fixture *f = data;
      GROW;
      ADD(0, 3, 0);
      CLUSTER_STEP_UNTIL_APPLIED(0, 2, 2000);
      ASSIGN(0, 3, RAFT_STANDBY);
      CLUSTER_STEP_UNTIL_APPLIED(2, 1, 2000);
      CLUSTER_STEP_N(2);
      REMOVE(0, 3, 0);
      ASSERT_CONFIGURATION_INDEXES(0, 3, 4);
      CLUSTER_STEP_UNTIL_APPLIED(0, 4, 2000);
      ASSERT_CONFIGURATION_INDEXES(0, 4, 0);
      munit_assert_int(CLUSTER_RAFT(0)->configuration.n, ==, 2);
    */
    return MUNIT_OK;
}

/* A leader gets a request to remove itself. */
TEST(raft_remove, self, setup, tear_down, 0, NULL)
{
    /*
      struct fixture *f = data;
      REMOVE(0, 1, 0);
      CLUSTER_STEP_UNTIL_APPLIED(0, 2, 2000);
    */
    /* TODO: the second server does not get notified */
    return MUNIT_SKIP;
    // CLUSTER_STEP_UNTIL_APPLIED(1, 2, 2000);
    return MUNIT_OK;
}

/* Trying to remove a server on a node which is not the leader results in an
 * error. */
TEST(raft_remove, notLeader, setup, tear_down, 0, NULL)
{
    // struct fixture *f = data;
    // REMOVE(1 /*   I                                                     */,
    //      3 /*   ID                                                    */,
    //       RAFT_NOTLEADER);
    return MUNIT_OK;
}

/* Trying to remove a server while a configuration change is already in progress
 * results in an error. */
TEST(raft_remove, inProgress, setup, tear_down, 0, NULL)
{
    /*
      struct fixture *f = data;
      ADD(0, 3, 0);
      REMOVE(0, 3, RAFT_CANTCHANGE);
    */
    return MUNIT_OK;
}

/* Trying to remove a server with an unknwon ID results in an error. */
TEST(raft_remove, badId, setup, tear_down, 0, NULL)
{
    /*
      struct fixture *f = data;
      REMOVE(0, 3, RAFT_BADID);
    */
    return MUNIT_OK;
}
