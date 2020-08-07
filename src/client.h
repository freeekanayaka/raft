/* Handle client requests. */

#ifndef CLIENT_H_
#define CLIENT_H_

#include "../include/raft.h"

int clientAccept(struct raft *r, struct raft_entry *entries, unsigned n);

int clientSnapshot(struct raft *r, raft_index index);

int clientTransfer(struct raft *r, raft_id id);

#endif /* CLIENT_H_ */
