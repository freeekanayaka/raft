/* Log replication logic and helpers. */

#ifndef REPLICATION_H_
#define REPLICATION_H_

#include "../include/raft.h"

/* Send empty AppendEntries RPC messages to all followers to which no
 * AppendEntries was sent in the last heartbeat interval.
 *
 * If the initial flag is true, this is the very first heartbeat of the term
 * right after election, in that case also send a black no-op entry.
 *
 * From Section 6.4:
 *
 *  1. If the leader has not yet marked an entry from its current term
 *     committed, it waits until it has done so. The Leader Completeness
 *     Property guarantees that a leader has all committedentries, but at the
 *     start of its term, it may not know which those are. To find out, it needs
 *     to commit an entry from its term. Raft handles this by having each leader
 *     commit a blank no-op entry into the log at the start of its term. As soon
 *     as this no-op entry is committed, the leader's commit index will be at
 *     least as large as any other servers' during its term.
 */
int replicationHeartbeat(struct raft *r, bool initial);

/* Start a local disk write for entries from the given index onwards, and
 * trigger replication against all followers, typically sending AppendEntries
 * RPC messages with outstanding log entries. */
int replicationTrigger(struct raft *r, raft_index index);

/* Send an AppendEntries or an InstallSnapshot RPC message to the server with
 * the given index, according to the value of it's nextIndex.
 *
 * The rules to decide what type of message to send and what it should contain
 * are:
 *
 * - If we don't have anymore the entry at nextIndex that should be sent to the
 *   follower, then send an InstallSnapshot RPC with the last snapshot.
 *
 * - If we still have the first entry to send, then send all entries from that
 *   index onward (possibly zero).
 *
 * This function must be called only by leaders. */
int replicationProgress(struct raft *r, unsigned i);

/* Update the replication state (match and next indexes) for the given server
 * using the given AppendEntries RPC result.
 *
 * Possibly send to the server a new set of entries or a snapshot if the result
 * was unsuccessful because of missing entries or if new entries were added to
 * our log in the meantime.
 *
 * It must be called only by leaders. */
int replicationUpdate(struct raft *r,
                      const struct raft_server *server,
                      const struct raft_append_entries_result *result);

/* Append the log entries in the given request if the Log Matching Property is
 * satisfied.
 *
 * The rejected output parameter will be set to 0 if the Log Matching Property
 * was satisfied, or to args->prev_log_index if not.
 *
 * The async output parameter will be set to true if some of the entries in the
 * request were not present in our log, and a disk write was started to persist
 * them to disk. The entries will still be appended immediately to our in-memory
 * copy of the log, but an AppendEntries result message will be sent only once
 * the disk write completes and the I/O callback is invoked.
 *
 * It must be called only by followers. */
int replicationAppend(struct raft *r,
                      const struct raft_append_entries *args,
                      raft_index *rejected,
                      bool *async);

int replicationInstallSnapshot(struct raft *r,
                               const struct raft_install_snapshot *args,
                               raft_index *rejected,
                               bool *async);

/* Apply any committed entry that was not applied yet.
 *
 * It must be called by leaders or followers. */
int replicationApply(struct raft *r);

/* Callback invoked when an append entries message send request has been
 * processed. */
void replicationSendAppendEntriesDone(struct raft *r,
                                      struct raft_send_message *send,
                                      int status);

/* Callback invoked when an install snapshot message has been processed. */
void replicationSendInstallSnapshotDone(struct raft *r,
                                        struct raft_send_message *send,
                                        int status);

/* Callback invoked when a persist entries request has been processed. */
void replicationPersistEntriesDone(struct raft *r,
                                   struct raft_persist_entries *persist,
                                   int status);

/* Callback invoked when a persist snapshot request has been processed. */
void replicationPersistSnapshotDone(struct raft *r,
                                    struct raft_persist_snapshot *persist,
                                    int status);

#endif /* REPLICATION_H_ */
