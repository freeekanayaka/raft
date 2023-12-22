#include "../include/raft.h"
#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "entry.h"
#include "err.h"
#include "legacy.h"
#include "log.h"
#include "recv.h"
#include "snapshot.h"
#include "tick.h"
#include "tracing.h"

#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

/* Restore the most recent configuration entry found in the log. */
static int restoreMostRecentConfigurationEntry(struct raft *r,
                                               struct raft_entry *entry,
                                               raft_index index)
{
    struct raft_configuration configuration;
    int rv;

    rv = configurationDecode(&entry->buf, &configuration);
    if (rv != 0) {
        configurationClose(&configuration);
        return rv;
    }

    configurationClose(&r->configuration);
    r->configuration = configuration;

    /* If the configuration comes from entry at index 1 in the log, we know it's
     * the bootstrap configuration and it's committed by default. Otherwise we
     * we can't know if it's committed or not and treat it as uncommitted. */
    if (index == 1) {
        assert(r->configuration_uncommitted_index == 0);
        r->configuration_committed_index = 1;
    } else {
        assert(r->configuration_committed_index < index);
        r->configuration_uncommitted_index = index;
    }

    configurationTrace(r, &r->configuration,
                       "restore most recent configuration");
    return 0;
}

/* Note that if the last configuration entry in the log has index greater than
 * one we cannot know if it is committed or not. Therefore we also need to track
 * the second-to-last configuration entry. This second-to-last entry is
 * committed by default as raft doesn't allow multiple uncommitted configuration
 * entries. That entry is used in case of configuration rollback scenarios. If
 * we don't find the second-to-last configuration entry in the log, it means
 * that the log was truncated after a snapshot and second-to-last configuration
 * is available in r->configuration_last_snapshot, which we popolated earlier
 * when the snapshot was restored. */
int RestoreEntries(struct raft *r,
                   raft_index snapshot_index,
                   raft_term snapshot_term,
                   raft_index start_index,
                   struct raft_entry *entries,
                   unsigned n)
{
    struct raft_entry *conf = NULL;
    raft_index conf_index = 0;
    unsigned i;
    int rv;
    logStart(r->log, snapshot_index, snapshot_term, start_index);
    r->last_stored = start_index - 1;
    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        rv = logAppend(r->log, entry->term, entry->type, &entry->buf,
                       entry->batch);
        if (rv != 0) {
            goto err;
        }
        r->last_stored++;

        /* Only take into account configurations that are newer than the
         * configuration restored from the snapshot. */
        if (entry->type == RAFT_CHANGE &&
            r->last_stored > r->configuration_committed_index) {
            /* If there is a previous configuration it must have been committed
             * as we don't allow multiple uncommitted configurations. At the end
             * of the loop r->configuration_committed_index will point to the
             * second to last configuration entry, if any. */
            if (conf_index != 0) {
                r->configuration_committed_index = conf_index;
            }
            conf = entry;
            conf_index = r->last_stored;
        }
    }

    if (conf != NULL) {
        rv = restoreMostRecentConfigurationEntry(r, conf, conf_index);
        if (rv != 0) {
            goto err;
        }
    }

    raft_free(entries);
    return 0;

err:
    if (logNumEntries(r->log) > 0) {
        logDiscard(r->log, r->log->offset + 1);
    }
    return rv;
}

int RestoreSnapshot(struct raft *r, struct raft_snapshot_metadata *metadata)
{
    int rv;

    configurationClose(&r->configuration);
    r->configuration = metadata->configuration;
    r->configuration_committed_index = metadata->configuration_index;
    r->configuration_uncommitted_index = 0;

    /* Make a copy of the configuration contained in the snapshot, in case
     * r->configuration gets overriden with an uncommitted configuration and we
     * then need to rollback, but the log does not contain anymore the entry at
     * r->configuration_committed_index because it was truncated. */
    configurationClose(&r->configuration_last_snapshot);
    rv = configurationCopy(&r->configuration, &r->configuration_last_snapshot);
    if (rv != 0) {
        return rv;
    }

    /* Make also a copy of the index of the configuration contained in the
     * snapshot, we'll need it in case we send out an InstallSnapshot RPC. */
    r->configuration_last_snapshot_index = metadata->configuration_index;

    configurationTrace(r, &r->configuration,
                       "configuration restore from snapshot");

    r->commit_index = metadata->index;
    r->last_applied = metadata->index;
    r->last_stored = metadata->index;

    return 0;
}

/* If we're the only voting server in the configuration, automatically
 * self-elect ourselves and convert to leader without waiting for the election
 * timeout. */
static int maybeSelfElect(struct raft *r)
{
    const struct raft_server *server;
    int rv;
    server = configurationGet(&r->configuration, r->id);
    if (server == NULL || server->role != RAFT_VOTER ||
        configurationVoterCount(&r->configuration) > 1) {
        return 0;
    }
    /* Converting to candidate will notice that we're the only voter and
     * automatically convert to leader. */
    rv = convertToCandidate(r, false /* disrupt leader */);
    if (rv != 0) {
        return rv;
    }
    assert(r->state == RAFT_LEADER);
    return 0;
}

int raft_start(struct raft *r)
{
    struct raft_snapshot *snapshot;
    struct raft_snapshot_metadata metadata;
    raft_index snapshot_index = 0;
    raft_term snapshot_term = 0;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n_entries;
    struct raft_event event;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_UNAVAILABLE);
    assert(r->heartbeat_timeout != 0);
    assert(r->heartbeat_timeout < r->election_timeout);
    assert(r->install_snapshot_timeout != 0);
    assert(logNumEntries(r->log) == 0);
    assert(logSnapshotIndex(r->log) == 0);
    assert(r->last_stored == 0);

    tracef("starting");
    rv = r->io->load(r->io, &r->current_term, &r->voted_for, &snapshot,
                     &start_index, &entries, &n_entries);
    if (rv != 0) {
        ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
        return rv;
    }
    assert(start_index >= 1);
    tracef("current_term:%llu voted_for:%llu start_index:%llu n_entries:%zu",
           r->current_term, r->voted_for, start_index, n_entries);

    /* If we have a snapshot, let's restore it. */
    if (snapshot != NULL) {
        tracef("restore snapshot with last index %llu and last term %llu",
               snapshot->index, snapshot->term);

        /* Save the snapshot data in the cache, it will be used by legacy compat
         * code to avoid loading the snapshot asynchronously. */
        rv = r->fsm->restore(r->fsm, &snapshot->bufs[0]);
        if (rv != 0) {
            tracef("restore snapshot %llu: %s", snapshot->index,
                   errCodeToString(rv));
            snapshotDestroy(snapshot);
            entryBatchesDestroy(entries, n_entries);
            return rv;
        }

        metadata.index = snapshot->index;
        metadata.term = snapshot->term;
        metadata.configuration = snapshot->configuration;
        metadata.configuration_index = snapshot->configuration_index;
        rv = RestoreSnapshot(r, &metadata);
        if (rv != 0) {
            entryBatchesDestroy(entries, n_entries);
            return rv;
        }

        raft_free(snapshot->bufs);
        snapshot_index = snapshot->index;
        snapshot_term = snapshot->term;
        raft_free(snapshot);

    } else if (n_entries > 0) {
        /* If we don't have a snapshot and the on-disk log is not empty, then
         * the first entry must be a configuration entry. */
        assert(start_index == 1);
        assert(entries[0].type == RAFT_CHANGE);

        /* As a small optimization, bump the commit index to 1 since we require
         * the first entry to be the same on all servers. */
        r->commit_index = 1;
        r->last_applied = 1;
    }

    /* Append the entries to the log, possibly restoring the last
     * configuration. */
    tracef("restore %zu entries starting at %llu", n_entries, start_index);
    rv = RestoreEntries(r, snapshot_index, snapshot_term, start_index, entries,
                        (unsigned)n_entries);
    if (rv != 0) {
        entryBatchesDestroy(entries, n_entries);
        return rv;
    }

    /* Start the I/O backend. The tickCb function is expected to fire every
     * r->heartbeat_timeout milliseconds and recvCb whenever an RPC is
     * received. */
    rv = r->io->start(r->io, r->heartbeat_timeout, tickCb, recvCb);
    if (rv != 0) {
        tracef("io start failed %d", rv);
        return rv;
    }

    /* By default we start as followers. */
    convertToFollower(r);

    /* If there's only one voting server, and that is us, it's safe to convert
     * to leader right away. If that is not us, we're either joining the cluster
     * or we're simply configured as non-voter, and we'll stay follower. */
    rv = maybeSelfElect(r);
    if (rv != 0) {
        return rv;
    }

    /* Use a dummy event to trigger handling of possible RAFT_APPLY_COMMAND
     * tasks.
     *
     * TODO: use the start event instead. */
    event.type = 255;
    event.time = r->now;
    LegacyForwardToRaftIo(r, &event);

    return 0;
}

#undef tracef