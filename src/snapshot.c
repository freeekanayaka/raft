#include "snapshot.h"

#include <stdint.h>
#include <string.h>

#include "assert.h"
#include "configuration.h"
#include "err.h"
#include "log.h"
#include "tracing.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

void snapshotClose(struct raft_snapshot *s)
{
    /* configurationClose(&s->configuration); */
    raft_free(s->data.base);
}

void snapshotDestroy(struct raft_snapshot *s)
{
    snapshotClose(s);
    raft_free(s);
}

void snapshotRestore(struct raft *r, struct raft_snapshot *snapshot)
{
    configurationClose(&r->configuration);
    r->configuration = snapshot->configuration;
    r->configuration_index = snapshot->configuration_index;

    r->commit_index = snapshot->index;
    r->last_applied = snapshot->index;
    r->last_stored = snapshot->index;

    r->snapshot.data = snapshot->data;
}

#undef tracef
