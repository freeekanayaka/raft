#include "election.h"

#include "assert.h"
#include "configuration.h"
#include "err.h"
#include "heap.h"
#include "io.h"
#include "log.h"
#include "prng.h"
#include "queue.h"
#include "tracing.h"

#define tracef(...) Tracef(r->tracer, "  " __VA_ARGS__)

/* Common fields between follower and candidate state.
 *
 * The follower_state and candidate_state structs in raft.h must be kept
 * consistent with this definition. */
struct followerOrCandidateState
{
    unsigned randomized_election_timeout;
};

/* Return a pointer to either the follower or candidate state. */
struct followerOrCandidateState *getFollowerOrCandidateState(struct raft *r)
{
    struct followerOrCandidateState *state;
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
    if (r->state == RAFT_FOLLOWER) {
        state = (struct followerOrCandidateState *)&r->follower_state;
    } else {
        state = (struct followerOrCandidateState *)&r->candidate_state;
    }
    return state;
}

void electionUpdateRandomizedTimeout(struct raft *r)
{
    struct followerOrCandidateState *state = getFollowerOrCandidateState(r);
    unsigned timeout = (unsigned)prngRange(&r->prng, (int)r->election_timeout,
                                           2 * (int)r->election_timeout);
    assert(timeout >= r->election_timeout);
    assert(timeout <= r->election_timeout * 2);
    state->randomized_election_timeout = timeout;
    r->clock->timeout(r->clock, (unsigned)(r->election_timer_start + timeout -
                                           r->clock->now(r->clock)));
}

void electionResetTimer(struct raft *r)
{
    r->election_timer_start = r->clock->now(r->clock);
    electionUpdateRandomizedTimeout(r);
}

bool electionTimerExpired(struct raft *r)
{
    struct followerOrCandidateState *state = getFollowerOrCandidateState(r);
    raft_time now = r->clock->now(r->clock);
    return now - r->election_timer_start >= state->randomized_election_timeout;
}

/* Send a RequestVote RPC to the given server. */
static int electionSend(struct raft *r, const struct raft_server *server)
{
    struct raft_message message;
    raft_term term;
    int rv;
    assert(server->id != r->id);
    assert(server->id != 0);

    /* If we are in the pre-vote phase, we indicate our future term in the
     * request. */
    term = r->current_term;
    if (r->candidate_state.in_pre_vote) {
        term++;
    }

    message.type = RAFT_IO_REQUEST_VOTE;
    message.request_vote.term = r->current_term;
    message.request_vote.candidate_id = r->id;
    message.request_vote.last_log_index = logLastIndex(&r->log);
    message.request_vote.last_log_term = logLastTerm(&r->log);
    message.request_vote.disrupt_leader = r->candidate_state.disrupt_leader;
    message.request_vote.pre_vote = r->candidate_state.in_pre_vote;

    rv = ioSendMessage(r, server->id, server->address, message);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int electionStart(struct raft *r)
{
    raft_term term;
    size_t n_voters;
    size_t voting_index;
    size_t i;
    int rv;
    assert(r->state == RAFT_CANDIDATE);

    n_voters = configurationVoterCount(&r->configuration);
    voting_index = configurationIndexOfVoter(&r->configuration, r->id);

    /* This function should not be invoked if we are not a voting server, hence
     * voting_index must be lower than the number of servers in the
     * configuration (meaning that we are a voting server). */
    assert(voting_index < r->configuration.n);

    /* Sanity check that configurationVoterCount and configurationIndexOfVoter
     * have returned somethig that makes sense. */
    assert(n_voters <= r->configuration.n);
    assert(voting_index < n_voters);

    /* During pre-vote we don't actually increment term or persist vote, however
     * we reset any vote that we previously granted since we have timed out and
     * that vote is no longer valid. */
    if (r->candidate_state.in_pre_vote) {
        /* Reset vote */
        rv = ioPersistTermAndVote(r, r->current_term, 0);
        if (rv != 0) {
            goto err;
        }
        /* Update our cache too. */
        r->voted_for = 0;
    } else {
        /* Increment current term */
        term = r->current_term + 1;
        rv = ioPersistTermAndVote(r, term, r->id);
        if (rv != 0) {
            goto err;
        }

        /* Update our cache too. */
        r->current_term = term;
        r->voted_for = r->id;
    }

    /* Reset election timer. */
    electionResetTimer(r);

    assert(r->candidate_state.votes != NULL);

    /* Initialize the votes array and send vote requests. */
    for (i = 0; i < n_voters; i++) {
        if (i == voting_index) {
            r->candidate_state.votes[i] = true; /* We vote for ourselves */
        } else {
            r->candidate_state.votes[i] = false;
        }
    }
    for (i = 0; i < r->configuration.n; i++) {
        const struct raft_server *server = &r->configuration.servers[i];
        if (server->id == r->id || server->role != RAFT_VOTER) {
            continue;
        }
        rv = electionSend(r, server);
        if (rv != 0) {
            /* This is not a critical failure, let's just log it. */
            tracef("failed to send vote request to server %llu: %s", server->id,
                   errCodeToString(rv));
        }
    }

    return 0;

err:
    assert(rv != 0);
    return rv;
}

int electionVote(struct raft *r,
                 const struct raft_request_vote *args,
                 bool *granted)
{
    const struct raft_server *local_server;
    raft_index local_last_index;
    raft_term local_last_term;
    bool is_transferee; /* Requester is the target of a leadership transfer */
    int rv;

    assert(r != NULL);
    assert(args != NULL);
    assert(granted != NULL);

    local_server = configurationGet(&r->configuration, r->id);

    *granted = false;

    if (local_server == NULL || local_server->role != RAFT_VOTER) {
        tracef("local server is not voting -> don't grant vote");
        return 0;
    }

    is_transferee = r->transfer.id != 0 && r->transfer.id == args->candidate_id;
    if (r->voted_for != 0 && r->voted_for != args->candidate_id &&
        !is_transferee) {
        tracef("local server already voted for %llu -> don't grant vote",
               r->voted_for);
        return 0;
    }

    local_last_index = logLastIndex(&r->log);

    /* Our log is definitely not more up-to-date if it's empty! */
    if (local_last_index == 0) {
        tracef("local log is empty -> grant vote");
        goto grant_vote;
    }

    local_last_term = logLastTerm(&r->log);

    if (args->last_log_term < local_last_term) {
        /* The requesting server has last entry's log term lower than ours. */
        tracef(
            "remote log older (%llu/%llu vs %llu/%llu) -> don't "
            "grant vote",
            args->last_log_index, args->last_log_term, local_last_index,
            local_last_term);
        return 0;
    }

    if (args->last_log_term > local_last_term) {
        /* The requesting server has a more up-to-date log. */
        tracef(
            "remote log has higher term (%llu/%llu vs %llu/%llu) -> "
            "grant vote",
            args->last_log_index, args->last_log_term, local_last_index,
            local_last_term);
        goto grant_vote;
    }

    /* The term of the last log entry is the same, so let's compare the length
     * of the log. */
    assert(args->last_log_term == local_last_term);

    if (args->last_log_index >= local_last_index) {
        /* Our log is shorter or equal to the one of the requester. */
        tracef(
            "remote log equal or longer (%llu/%llu vs %llu/%llu) -> grant vote",
            args->last_log_index, args->last_log_term, local_last_index,
            local_last_term);
        goto grant_vote;
    }

    tracef(
        "remote log shorter (%llu/%llu vs %llu/%llu) -> don't grant "
        "vote",
        args->last_log_index, args->last_log_term, local_last_index,
        local_last_term);

    return 0;

grant_vote:
    if (!args->pre_vote) {
        rv = ioPersistTermAndVote(r, r->current_term, args->candidate_id);
        if (rv != 0) {
            return rv;
        }
        /* Update our cache too. */
        r->voted_for = args->candidate_id;

        /* Reset the election timer. */
        r->election_timer_start = r->clock->now(r->clock);
        r->clock->timeout(r->clock,
                          r->follower_state.randomized_election_timeout);
    }

    *granted = true;

    return 0;
}

bool electionTally(struct raft *r, size_t voter_index)
{
    size_t n_voters = configurationVoterCount(&r->configuration);
    size_t votes = 0;
    size_t i;
    size_t half = n_voters / 2;

    assert(r->state == RAFT_CANDIDATE);
    assert(r->candidate_state.votes != NULL);

    r->candidate_state.votes[voter_index] = true;

    for (i = 0; i < n_voters; i++) {
        if (r->candidate_state.votes[i]) {
            votes++;
        }
    }

    return votes >= half + 1;
}

#undef tracef
