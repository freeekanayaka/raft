#include "driver.h"

static void testFileInit(struct TestFile *f, const char *name)
{
    strcpy(f->name, name);
    f->data.base = NULL;
    f->data.len = 0;
}

static void testFileClose(struct TestFile *f)
{
    free(f->data.base);
}

static void testFileOverwrite(struct TestFile *f, struct raft_buffer *data)
{
    if (f->data.base != NULL) {
        free(f->data.base);
    }
    f->data.len = data->len;
    f->data.base = munit_malloc(f->data.len);
    memcpy(f->data.base, data->base, f->data.len);
}

static void testFileRead(struct TestFile *f, struct raft_buffer *data)
{
    if (f->data.base == NULL) {
        data->len = 0;
        data->base = NULL;
        return;
    }

    data->len = f->data.len;
    data->base = raft_malloc(data->len);
    munit_assert_ptr_not_null(data->base);
    memcpy(data->base, f->data.base, data->len);
}

static void testDiskInit(struct TestDisk *d)
{
    d->files = NULL;
    d->n_files = 0;
}

static void testDiskClose(struct TestDisk *d)
{
    unsigned i;
    for (i = 0; i < d->n_files; i++) {
        testFileClose(&d->files[i]);
    }
    if (d->files != NULL) {
        free(d->files);
    }
}

/* Find the file with the given name, if any. */
static struct TestFile *testDiskFind(struct TestDisk *d, const char *name)
{
    unsigned i;
    for (i = 0; i < d->n_files; i++) {
        struct TestFile *file = &d->files[i];
        if (strcmp(file->name, name) == 0) {
            return file;
        }
    }
    return NULL;
}

/* Create a new file with no content. */
static struct TestFile *testDiskCreate(struct TestDisk *d, const char *name)
{
    struct TestFile *file;

    d->n_files++;
    d->files = reallocarray(d->files, d->n_files, sizeof *d->files);
    munit_assert_ptr_not_null(d->files);

    file = &d->files[d->n_files - 1];
    testFileInit(file, name);

    return file;
}

/* Return an array of all filenames. */
static const char **testDiskFilenames(struct TestDisk *d)
{
    const char **names;
    unsigned i;

    names = munit_malloc(sizeof *names * d->n_files);
    for (i = 0; i < d->n_files; i++) {
        names[i] = d->files[i].name;
    }

    return names;
}

static void testDiskWriteFile(struct TestDisk *d,
                              const char *name,
                              struct raft_buffer *data)
{
    struct TestFile *file = testDiskFind(d, name);

    if (file == NULL) {
        file = testDiskCreate(d, name);
    }

    testFileOverwrite(file, data);
}

static void testDriverEmit(struct raft_tracer *t,
                           const char *file,
                           int line,
                           const char *message)
{
    (void)t;
    (void)line;
    (void)file;
    fprintf(stderr, "%s\n", message);
}

void TestDriverSetUp(const MunitParameter params[], struct TestDriver *b)
{
    int rv;
    (void)params;
    testDiskInit(&b->disk);
    b->tracer.data = NULL;
    b->tracer.emit = testDriverEmit;
    rv = raft_backend_init(&b->backend);
    munit_assert_int(rv, ==, 0);
    b->backend.tracer = &b->tracer;
}

void TestDriverTearDown(struct TestDriver *b)
{
    raft_backend_close(&b->backend);
    testDiskClose(&b->disk);
}

static int testDriverHandleLoadIoRead(struct TestDriver *b,
                                      struct raft_backend_load_io *io)
{
    struct TestFile *file = testDiskFind(&b->disk, io->filename);
    struct raft_buffer data;

    if (file == NULL) {
        return raft_backend_load_read(&b->backend, io->filename, NULL);
    }

    testFileRead(file, &data);

    return raft_backend_load_read(&b->backend, io->filename, &data);
}

static int testDriverHandleLoadIo(struct TestDriver *b)
{
    struct raft_backend_load_io *io;
    int rv;

    while ((io = raft_backend_load_next(&b->backend)) != NULL) {
        switch (io->type) {
            case RAFT_BACKEND_LOAD_READ:
                rv = testDriverHandleLoadIoRead(b, io);
                break;
            default:
                rv = 0;
                break;
        }
        raft_free(io);
        if (rv != 0) {
            return rv;
        }
    }

    return 0;
}

int TestDriverLoad(struct TestDriver *b,
                   raft_term *term,
                   raft_id *voted_for,
                   struct raft_snapshot **snapshot,
                   raft_index *start_index,
                   struct raft_entry **entries,
                   unsigned *n_entries)
{
    const char **names;
    int rv;

    names = testDiskFilenames(&b->disk);

    rv = raft_backend_load_scan(&b->backend, names, b->disk.n_files);
    if (rv != 0) {
        goto out;
    }

    rv = testDriverHandleLoadIo(b);
    if (rv != 0) {
        goto out;
    }

    rv = raft_backend_load_done(&b->backend, term, voted_for, snapshot,
                                start_index, entries, n_entries);

out:
    if (names != NULL) {
        free(names);
    }

    return rv;
}

int TestDriverPersistTermAndVote(struct TestDriver *b,
                                 raft_term term,
                                 raft_id voted_for)
{
    const char *filename;
    struct raft_buffer content;
    int rv;
    rv = raft_backend_persist_term_and_vote(&b->backend, term, voted_for,
                                            &filename, &content);
    if (rv != 0) {
        return rv;
    }

    testDiskWriteFile(&b->disk, filename, &content);

    return 0;
}

const char *TestDriverErrMsg(struct TestDriver *b)
{
    return raft_backend_errmsg(&b->backend);
}
