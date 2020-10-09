#include "../lib/driver.h"
#include "../lib/runner.h"

struct fixture
{
    DRIVER_FIXTURE;
};

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    DRIVER_SET_UP;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    DRIVER_TEAR_DOWN;
    free(f);
}

#define LOAD(TERM, VOTED_FOR, SNAPSHOT, START_INDEX, DATA, N_ENTRIES)     \
    do {                                                                  \
        raft_term _term;                                                  \
        raft_id _voted_for;                                               \
        struct raft_snapshot *_snapshot;                                  \
        raft_index _start_index;                                          \
        struct raft_entry *_entries;                                      \
        unsigned _n_entries;                                              \
        int _rv;                                                          \
        _rv = TestDriverLoad(&f->driver, &_term, &_voted_for, &_snapshot, \
                             &_start_index, &_entries, &_n_entries);      \
        munit_assert_int(_rv, ==, 0);                                     \
        munit_assert_int(_term, ==, TERM);                                \
        munit_assert_int(_voted_for, ==, VOTED_FOR);                      \
        munit_assert_int(_start_index, ==, START_INDEX);                  \
        munit_assert_int(_n_entries, ==, N_ENTRIES);                      \
    } while (0)

SUITE(load)

TEST(load, emptyDir, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         0,    /* data for first loaded entry    */
         0     /* n entries                                         */
    );
    return MUNIT_OK;
}

TEST(load, onlyMetadata1, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int rv;
    rv = TestDriverPersistTermAndVote(&f->driver, 1, 0);
    munit_assert_int(rv, ==, 0);
    LOAD(1,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         0,    /* data for first loaded entry    */
         0     /* n entries                                         */
    );
    return MUNIT_OK;
}
