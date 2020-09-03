#include "recv_timeout_now.h"

#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "log.h"
#include "recv.h"
#include "tracing.h"

/* Set to 1 to enable tracing. */
#define tracef(...) Tracef(r->tracer, "  " __VA_ARGS__)

int recvTimeoutNow(struct raft *r,
                   const raft_id id,
                   const char *address,
                   const struct raft_timeout_now *args)
{
    const struct raft_server *local_server;
    raft_index local_last_index;
    raft_term local_last_term;
    int match;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);

    (void)address;

    /* Ignore the request if we are not voters. */
    local_server = configurationGet(&r->configuration, r->id);
    if (local_server == NULL || local_server->role != RAFT_VOTER) {
        tracef("not voter -> ignore");
        return 0;
    }

    /* Ignore the request if we are not follower, or we have different
     * leader. */
    if (r->state != RAFT_FOLLOWER ||
        r->follower_state.current_leader.id != id) {
        tracef("not follower or different leader -> ignore");
        return 0;
    }

    /* Possibly update our term. Ignore the request if it turns out we have a
     * higher term. */
    rv = recvEnsureMatchingTerms(r, args->term, &match);
    if (rv != 0) {
        return rv;
    }
    if (match < 0) {
        tracef("local term %llu higher than %llu -> ignore", r->current_term,
               args->term);
        return 0;
    }

    /* Ignore the request if we our log is not up-to-date. */
    local_last_index = logLastIndex(&r->log);
    local_last_term = logLastTerm(&r->log);
    if (local_last_index != args->last_log_index ||
        local_last_term != args->last_log_term) {
        tracef("local log not up-to-date -> ignore");
        return 0;
    }

    /* Convert to candidate and start a new election. */
    tracef("convert to candidate and start new election for term %llu",
           r->current_term + 1);
    rv = convertToCandidate(r, true /* disrupt leader */);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

#undef tracef
