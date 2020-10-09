#ifndef TEST_DRIVER_H
#define TEST_DRIVER_H

#include "../../include/raft/backend.h"

#include "heap.h"
#include "munit.h"

#define DRIVER_FIXTURE \
    FIXTURE_HEAP;      \
    struct TestDriver driver

#define DRIVER_SET_UP \
    SET_UP_HEAP;      \
    TestDriverSetUp(params, &f->driver)

#define DRIVER_TEAR_DOWN            \
    TestDriverTearDown(&f->driver); \
    TEAR_DOWN_HEAP

struct TestFile
{
    char name[RAFT_BACKEND_FILENAME_MAX_LEN];
    struct raft_buffer data;
};

struct TestDisk
{
    struct TestFile *files;
    unsigned n_files;
};

struct TestDriver
{
    struct TestDisk disk;
    struct raft_tracer tracer;
    struct raft_backend backend;
};

void TestDriverSetUp(const MunitParameter params[], struct TestDriver *b);
void TestDriverTearDown(struct TestDriver *b);

int TestDriverLoad(struct TestDriver *b,
                   raft_term *term,
                   raft_id *voted_for,
                   struct raft_snapshot **snapshot,
                   raft_index *start_index,
                   struct raft_entry **entries,
                   unsigned *n_entries);

int TestDriverPersistTermAndVote(struct TestDriver *b,
                                 raft_term term,
                                 raft_id voted_for);

const char *TestDriverErrMsg(struct TestDriver *b);

#endif /* TEST_DRIVER_H */
