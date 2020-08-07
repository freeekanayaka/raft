/* Enqueue I/O requests. */

#ifndef IO_H_
#define IO_H_

#include "../include/raft.h"

#include "heap.h"
#include "queue.h"

int ioPersistTermAndVote(struct raft *r, raft_term term, raft_id voted_for);

int ioPersistEntries(struct raft *r,
                     raft_index first_index,
                     struct raft_entry *entries,
                     unsigned n);

int ioPersistSnapshot(struct raft *r, struct raft_snapshot *snapshot);

int ioSendMessage(struct raft *r,
                  raft_id id,
                  const char *address,
                  struct raft_message message);

#endif /* IO_H_ */
